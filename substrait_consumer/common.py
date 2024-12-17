from pathlib import Path

CUR_DIR = Path(__file__).parent


class SubstraitUtils:
    """
    Common utility for substrait integration tests.
    """

    @staticmethod
    def compute_full_paths(local_files: dict[str, str]) -> dict[str, str]:
        """
        Get the full paths for the given local files.

        Parameters:
            local_files:
                A `dict` mapping format argument names to local files paths.

        Returns:
            A `dict` where the paths are expanded to absolute paths.
        """
        data_dir = CUR_DIR / "data" / "tpch_parquet"
        return {k: f"{data_dir}/{v}" for k, v in local_files.items()}
