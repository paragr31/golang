import os
import tarfile
import hashlib
import time
import logging

class TarHelper:

    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)

    def make_tarfile(self, output_filename, source_dir, retries=3, retry_delay=0.5):
        file_size_sha256sum = {}

        # Ensure absolute paths
        output_filename = os.path.abspath(output_filename)
        source_dir = os.path.abspath(source_dir)

        try:
            with tarfile.open(output_filename, "w:gz") as tar:
                for root, dirs, files in os.walk(source_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, start=os.path.dirname(source_dir))

                        # Retry logic for network drive glitches
                        for attempt in range(retries):
                            if os.path.exists(file_path):
                                try:
                                    tar.add(file_path, arcname=arcname)
                                    break  # success
                                except FileNotFoundError:
                                    self.logger.warning(f"[{attempt+1}/{retries}] File vanished: {file_path}")
                                    time.sleep(retry_delay)
                            else:
                                self.logger.warning(f"[{attempt+1}/{retries}] Path missing: {file_path}")
                                time.sleep(retry_delay)
                        else:
                            self.logger.error(f"Skipping permanently missing file: {file_path}")

            self.logger.info(f"Successfully created compressed file: {output_filename}")

        except Exception as e:
            self.logger.error(f"Failed to create compressed file {output_filename}. Error: {e}")
            return file_size_sha256sum

        # Get compressed size
        try:
            file_size_sha256sum['COMPRESSED_SIZE'] = os.stat(output_filename).st_size
        except Exception as e:
            self.logger.error(f"Failed to get size of {output_filename}. Error: {e}")
            return file_size_sha256sum

        # Compute SHA256 checksum (streaming, not whole file in memory)
        try:
            sha256 = hashlib.sha256()
            with open(output_filename, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256.update(chunk)
            file_size_sha256sum['SHA256SUM'] = sha256.hexdigest()
        except Exception as e:
            self.logger.error(f"Failed to compute SHA256 for {output_filename}. Error: {e}")

        return file_size_sha256sum
