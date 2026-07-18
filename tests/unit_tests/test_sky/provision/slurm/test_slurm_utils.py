"""Unit tests for sky.provision.slurm.utils."""

import pickle

import pytest

from sky.provision.slurm import utils


class TestSlurmInstanceType:
    """Characterize the public virtual instance type value model."""

    @pytest.mark.parametrize('name,expected', [
        ('4CPU--16GB', (4.0, 16.0, None, None)),
        ('0.5CPU--1.5GB', (0.5, 1.5, None, None)),
        ('4CPU--16GB--A100-80GB:2', (4.0, 16.0, 2, 'A100-80GB')),
    ])
    def test_parse_and_render(self, name, expected):
        instance_type = utils.SlurmInstanceType.from_instance_type(name)

        assert (instance_type.cpus, instance_type.memory,
                instance_type.accelerator_count,
                instance_type.accelerator_type) == expected
        assert instance_type.name == name
        assert str(instance_type) == name

    @pytest.mark.parametrize('name', [
        '',
        '4CPU',
        '4CPU--16GB--H100:1.5',
        '4CPU--16GB--H100',
    ])
    def test_rejects_invalid_names(self, name):
        assert not utils.SlurmInstanceType.is_valid_instance_type(name)
        with pytest.raises(ValueError, match='Invalid instance name'):
            utils.SlurmInstanceType.from_instance_type(name)

    def test_from_resources_rounds_accelerators_and_preserves_repr(self):
        instance_type = utils.SlurmInstanceType.from_resources(
            cpus=1.5,
            memory=3.25,
            accelerator_count=1.2,
            accelerator_type='H100',
        )

        assert instance_type.name == '1.5CPU--3.2GB--H100:2'
        assert repr(instance_type) == (
            "SlurmInstanceType(cpus=1.5, memory=3.25, "
            "accelerator_count=2, accelerator_type='H100')")

    def test_historical_module_and_pickle_identity(self):
        instance_type = utils.SlurmInstanceType.from_instance_type(
            '4CPU--16GB--H100:1')

        assert utils.SlurmInstanceType.__module__ == (
            'sky.provision.slurm.utils')
        restored = pickle.loads(pickle.dumps(instance_type))
        assert type(restored) is utils.SlurmInstanceType
        assert restored.name == instance_type.name


class TestFormatSlurmDuration:
    """Test format_slurm_duration()."""

    @pytest.mark.parametrize('duration_seconds,expected', [
        (10000, '0-02:46:40'),
        (100000, '1-03:46:40'),
        (1000000, '11-13:46:40'),
        (None, 'UNLIMITED'),
    ])
    def test_format_slurm_duration(self, duration_seconds, expected):
        """Test format_slurm_duration with various inputs."""
        result = utils.format_slurm_duration(duration_seconds)
        assert result == expected


class TestValidateSbatchTime:
    """Test validate_sbatch_time()."""

    @pytest.mark.parametrize(
        'value',
        [
            '5',  # m (bare minutes)
            '1:30',  # m:s
            '4:00:00',  # h:m:s
            '1-0',  # d-h
            '1-12',  # d-h (multi-digit hour)
            '2-23:59',  # d-h:m
            '7-00:00:00',  # d-h:m:s
        ])
    def test_accepted_formats(self, value):
        # Should not raise. One sample per grammatical form.
        utils.validate_sbatch_time(value)

    @pytest.mark.parametrize('value', [
        '',
        'garbage',
        '1h',
        '1m30s',
        '1:2:3:4',
        '1.5',
        '-1',
        '1-2-3',
        ':30',
        '1:',
        ' 5',
        '5 ',
        '5\n',
    ])
    def test_invalid_formats_raise(self, value):
        with pytest.raises(ValueError, match='Invalid slurm.sbatch_options'):
            utils.validate_sbatch_time(value)


class TestGetIdentityFile:
    """Test get_identity_file() helper function."""

    @pytest.mark.parametrize(
        'ssh_config_dict,expected',
        [
            # Returns first file when multiple identity files are present
            ({
                'identityfile': ['/path/to/key1', '/path/to/key2']
            }, '/path/to/key1'),
            # Returns single identity file
            ({
                'identityfile': ['/home/user/.ssh/id_rsa']
            }, '/home/user/.ssh/id_rsa'),
            # Returns None when identityfile key is missing
            ({
                'hostname': 'example.com',
                'user': 'testuser'
            }, None),
            # Returns None when identityfile is an empty list
            ({
                'identityfile': []
            }, None),
            # Returns None when identityfile value is None
            ({
                'identityfile': None
            }, None),
        ])
    def test_get_identity_file(self, ssh_config_dict, expected):
        """Test get_identity_file with various SSH config inputs."""
        result = utils.get_identity_file(ssh_config_dict)
        assert result == expected
