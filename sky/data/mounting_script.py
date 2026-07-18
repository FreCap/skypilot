"""Generic remote lifecycle script generation for storage mounts."""

import shlex
import textwrap

from sky.skylet import constants
from sky.utils import command_runner


def get_mount_binary(mount_cmd: str) -> str:
    """Returns the mounting binary used by a mount command."""
    if 'goofys' in mount_cmd:
        return 'goofys'
    elif 'gcsfuse' in mount_cmd:
        return 'gcsfuse'
    elif 'blobfuse2' in mount_cmd:
        return 'blobfuse2'
    elif 'hf-mount' in mount_cmd:
        return 'hf-mount'
    else:
        assert 'rclone' in mount_cmd
        return 'rclone'


def get_mounting_script(
    mount_path: str,
    mount_cmd: str,
    install_cmd: str,
    version_check_cmd: str | None,
    mount_binary: str,
) -> str:
    """Generates the generic remote lifecycle script for a storage mount."""
    installed_check = f'[ -x "$(command -v {mount_binary})" ]'
    if version_check_cmd is not None:
        installed_check += f' && {version_check_cmd}'

    script = textwrap.dedent(f"""
        #!/usr/bin/env bash
        set -e

        {command_runner.ALIAS_SUDO_TO_EMPTY_FOR_ROOT_CMD}

        MOUNT_PATH=$(eval echo {mount_path})
        MOUNT_BINARY={mount_binary}

        # Check if path is already mounted
        if findmnt -rn -T "$MOUNT_PATH" >/dev/null 2>&1; then
            echo "Path already mounted - unmounting..."
            (command -v fusermount >/dev/null 2>&1 && fusermount -uz "$MOUNT_PATH") \
            || (command -v fusermount3 >/dev/null 2>&1 && fusermount3 -uz "$MOUNT_PATH") \
            || sudo umount -l "$MOUNT_PATH" || true
            # Ensure it's really gone (avoids races)
            for i in $(seq 1 20); do
                if ! findmnt -rn -T "$MOUNT_PATH" >/dev/null 2>&1; then break; fi
                sleep 0.2
            done
            echo "Successfully unmounted $MOUNT_PATH."
        fi

        # Install MOUNT_BINARY if not already installed
        if {installed_check}; then
          echo "$MOUNT_BINARY already installed. Proceeding..."
        else
          echo "Installing $MOUNT_BINARY..."
          {install_cmd}
        fi

        # Check if mount path exists
        if [ ! -d "$MOUNT_PATH" ]; then
          echo "Mount path $MOUNT_PATH does not exist. Creating..."
          sudo mkdir -p "$MOUNT_PATH"
          sudo chmod 777 "$MOUNT_PATH"
        else
            # If not a mountpoint and contains files, clean it to satisfy SkyPilot check
            if ! findmnt -rn -T "$MOUNT_PATH" >/dev/null 2>&1; then
                if [ -n "$(ls -A "$MOUNT_PATH" 2>/dev/null)" ]; then
                  echo "Cleaning non-empty mount path before mount..."
                  sudo bash -lc 'shopt -s dotglob nullglob; rm -rf --one-file-system -- '"$MOUNT_PATH"'/*' 2>/dev/null || true
                fi
            fi
        fi
        echo "Mounting $SOURCE_BUCKET to $MOUNT_PATH with $MOUNT_BINARY..."
        set +e
        {mount_cmd}
        MOUNT_EXIT_CODE=$?
        set -e
        if [ $MOUNT_EXIT_CODE -ne 0 ]; then
            echo "Mount failed with exit code $MOUNT_EXIT_CODE."
            if [ "$MOUNT_BINARY" = "goofys" ]; then
                echo "Looking for goofys log files..."
                # Find goofys log files in /tmp (created by mktemp -t goofys.XXXX.log)
                # Note: if /dev/log exists, goofys logs to syslog instead of a file
                GOOFYS_LOGS=$(ls -t /tmp/goofys.*.log 2>/dev/null | head -1)
                if [ -n "$GOOFYS_LOGS" ]; then
                    echo "=== Goofys log file contents ==="
                    cat "$GOOFYS_LOGS"
                    echo "=== End of goofys log file ==="
                else
                    echo "No goofys log file found in /tmp"
                fi
            elif [ "$MOUNT_BINARY" = "gcsfuse" ]; then
                echo "Looking for gcsfuse log files..."
                # Find gcsfuse log files in /tmp (created by mktemp -t gcsfuse.XXXX.log)
                GCSFUSE_LOGS=$(ls -t /tmp/gcsfuse.*.log 2>/dev/null | head -1)
                if [ -n "$GCSFUSE_LOGS" ]; then
                    echo "=== GCSFuse log file contents ==="
                    cat "$GCSFUSE_LOGS"
                    echo "=== End of gcsfuse log file ==="
                else
                    echo "No gcsfuse log file found in /tmp"
                fi
            elif [ "$MOUNT_BINARY" = "rclone" ]; then
                echo "Looking for rclone log files..."
                # Find rclone log files in ~/.sky/rclone_log/ (for MOUNT_CACHED mode)
                RCLONE_LOG_DIR={constants.RCLONE_MOUNT_CACHED_LOG_DIR}
                if [ -d "$RCLONE_LOG_DIR" ]; then
                    RCLONE_LOGS=$(ls -t "$RCLONE_LOG_DIR"/*.log 2>/dev/null | head -1)
                    if [ -n "$RCLONE_LOGS" ]; then
                        echo "=== Rclone log file contents ==="
                        tail -50 "$RCLONE_LOGS"
                        echo "=== End of rclone log file ==="
                    else
                        echo "No rclone log file found in $RCLONE_LOG_DIR"
                    fi
                else
                    echo "Rclone log directory $RCLONE_LOG_DIR not found"
                fi
            elif [ "$MOUNT_BINARY" = "hf-mount" ]; then
                echo "Looking for hf-mount log files..."
                # hf-mount writes logs under ~/.hf-mount/logs/.
                HF_MOUNT_LOG_DIR="$HOME/.hf-mount/logs"
                if [ -d "$HF_MOUNT_LOG_DIR" ]; then
                    HF_MOUNT_LOGS=$(ls -t "$HF_MOUNT_LOG_DIR"/*.log 2>/dev/null | head -1)
                    if [ -n "$HF_MOUNT_LOGS" ]; then
                        echo "=== hf-mount log file contents ==="
                        tail -50 "$HF_MOUNT_LOGS"
                        echo "=== End of hf-mount log file ==="
                    else
                        echo "No hf-mount log file found in $HF_MOUNT_LOG_DIR"
                    fi
                else
                    echo "hf-mount log directory $HF_MOUNT_LOG_DIR not found"
                fi
            fi
            # TODO(kevin): Print logs from blobfuse2, etc too for observability.
            exit $MOUNT_EXIT_CODE
        fi
        echo "Mounting done."
    """)

    return script


def get_mounting_command(script: str, script_id: int) -> str:
    """Wraps a rendered mount script in a temporary remote shell command."""
    script_path = f'~/.sky/mount_{script_id}.sh'
    return (f'echo {shlex.quote(script)} > {script_path} && '
            f'chmod +x {script_path} && '
            f'bash {script_path} && '
            f'rm {script_path}')
