"""Reports, and optionally deletes, orphaned SkyPilot AWS security groups.

Two separate leaks produced these:

1. The default group name used to embed a hostname hash, which on a server
   deployment is the API-server pod name. Every restart therefore minted a new
   shared group, and ``cleanup_ports`` never deletes a shared default group.
   This is the bulk of the leak and is fixed at the source by the stable
   ``DEFAULT_SECURITY_GROUP_NAME``; the groups already created still need
   removing once.
2. A per-cluster port group whose deletion ``cleanup_ports`` had to abandon. It
   can only wait ~66s for the terminated instance's network interface to
   detach, and in practice that is not long enough, so it logs "Please delete
   it manually" and never returns to it.

This is a one-off operator tool rather than automatic behaviour on purpose.
Deleting cloud resources on a schedule, from inside a provisioning or teardown
path, risks racing a launch that has created its group but not yet its
instance; a human running this and reading the plan cannot.

Safety rests on "no network interface references it", which is exactly the
condition that makes a group deletable:

- A running cluster's group is referenced by its instance's interface.
- A STOPPED cluster keeps its interface, and therefore its reference, so a
  cluster intended to be restarted is never a candidate.
- Security groups expose no creation timestamp, so an age-based guard is not
  expressible; the reference check is both stronger and cheaper.

Dry run by default. Nothing is deleted without ``--delete``.

Usage:
    python -m sky.utils.aws.reap_orphaned_security_groups \\
        --regions us-east-1,eu-central-1
    python -m sky.utils.aws.reap_orphaned_security_groups --all-regions
    python -m sky.utils.aws.reap_orphaned_security_groups \\
        --regions us-east-1 --delete
"""
import argparse
import collections

from sky.adaptors import aws
from sky.clouds import aws as aws_cloud
from sky.provision import constants

# Only groups SkyPilot created. A name prefix alone would risk another tool's
# similarly named group; every group SkyPilot creates carries this tag.
_SKYPILOT_TAG = 'skypilot'


def _orphaned_groups(region: str) -> list[dict]:
    """Groups in ``region`` that SkyPilot owns and nothing references."""
    client = aws.client('ec2', region_name=region)
    groups: list[dict] = []
    group_pages = client.get_paginator('describe_security_groups')
    for group_page in group_pages.paginate(Filters=[{
            'Name': f'tag:{_SKYPILOT_TAG}',
            'Values': ['true']
    }]):
        groups.extend(dict(g) for g in group_page['SecurityGroups'])
    if not groups:
        return []

    # One pass over interfaces per VPC rather than one call per group.
    referenced_by_vpc: dict[str, set] = collections.defaultdict(set)
    for vpc_id in {group['VpcId'] for group in groups}:
        interface_pages = client.get_paginator('describe_network_interfaces')
        for interface_page in interface_pages.paginate(Filters=[{
                'Name': 'vpc-id',
                'Values': [vpc_id]
        }]):
            for interface in interface_page['NetworkInterfaces']:
                for attached in interface.get('Groups', []):
                    referenced_by_vpc[vpc_id].add(attached['GroupId'])

    orphaned: list[dict] = []
    for group in groups:
        if group['GroupId'] in referenced_by_vpc[group['VpcId']]:
            continue
        orphaned.append(group)
    return orphaned


def _describe(group: dict) -> str:
    kind = ('shared-default' if aws_cloud.is_shared_default_security_group(
        group['GroupName']) else 'per-cluster')
    managed = any(tag['Key'] == constants.TAG_SKYPILOT_MANAGED
                  for tag in group.get('Tags', []))
    open_ssh = any(
        rule.get('FromPort') == 22 and any(r['CidrIp'] == '0.0.0.0/0'
                                           for r in rule.get('IpRanges', []))
        for rule in group.get('IpPermissions', []))
    return (f'{group["GroupName"]:52} {group["GroupId"]:22} {kind:15} '
            f'managed-tag={str(managed):5} ssh-open={open_ssh}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--regions', help='Comma-separated AWS regions.')
    group.add_argument('--all-regions',
                       action='store_true',
                       help='Every region this account has enabled.')
    parser.add_argument('--delete',
                        action='store_true',
                        help='Actually delete. Omit for a dry run.')
    args = parser.parse_args()

    if args.all_regions:
        client = aws.client('ec2', region_name='us-east-1')
        regions = [
            r['RegionName'] for r in client.describe_regions()['Regions']
        ]
    else:
        regions = [r.strip() for r in args.regions.split(',') if r.strip()]

    total = 0
    failed = 0
    for region in sorted(regions):
        try:
            orphaned = _orphaned_groups(region)
        except Exception as e:  # pylint: disable=broad-except
            print(f'{region}: SKIPPED ({type(e).__name__}: {e})')
            continue
        if not orphaned:
            print(f'{region}: none')
            continue
        print(f'{region}: {len(orphaned)} orphaned')
        for item in orphaned:
            print(f'    {_describe(item)}')
        total += len(orphaned)
        if not args.delete:
            continue
        client = aws.client('ec2', region_name=region)
        for item in orphaned:
            try:
                client.delete_security_group(GroupId=item['GroupId'])
                print(f'    deleted {item["GroupName"]}')
            except Exception as e:  # pylint: disable=broad-except
                # Most likely an interface attached between the scan and now,
                # which is precisely the case we must not force.
                failed += 1
                print(f'    KEPT {item["GroupName"]}: '
                      f'{type(e).__name__}: {e}')

    verb = 'deleted' if args.delete else 'would delete'
    print(f'\n{verb} {total - failed} group(s)'
          f'{f", {failed} kept" if failed else ""}.')
    if not args.delete:
        print('Dry run. Re-run with --delete to apply.')


if __name__ == '__main__':
    main()
