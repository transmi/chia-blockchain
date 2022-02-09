#!/usr/bin/env python3

import asyncio
import aiosqlite
import zstd
import click
import logging
import cProfile

from pathlib import Path
from time import time
import tempfile

from chia.types.full_block import FullBlock
from chia.consensus.default_constants import DEFAULT_CONSTANTS
from chia.util.config import load_config
from chia.full_node.full_node import FullNode

from chia.cmds.init_funcs import chia_init


exit_with_failure = False


class ExitOnError(logging.Handler):
    def handle(self, record):
        pass

    def emit(self, record):
        if record.level != logging.ERROR:
            return
        global exit_with_failure
        exit_with_failure = True

    def createLock(self):
        self.lock = None

    def _at_fork_reinit(self):
        pass


async def run_sync_test(file: Path, db_version, profile: bool) -> None:

    global exit_with_failure

    logger = logging.getLogger()
    logger.setLevel(logging.WARNING)
    handler = logging.FileHandler("test-full-sync.log")
    handler.setFormatter(
        logging.Formatter(
            "%(levelname)-8s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    logger.addHandler(handler)
    logger.addHandler(ExitOnError())

    with tempfile.TemporaryDirectory() as root_dir:

        root_path = Path(root_dir)
        chia_init(root_path, should_check_keys=False)
        config = load_config(root_path, "config.yaml")

        overrides = config["network_overrides"]["constants"][config["selected_network"]]
        constants = DEFAULT_CONSTANTS.replace_str_to_bytes(**overrides)
        full_node = FullNode(
            config["full_node"],
            root_path=root_path,
            consensus_constants=constants,
        )

        try:
            await full_node._start()

            print()
            counter = 0
            async with aiosqlite.connect(file) as in_db:

                rows = await in_db.execute("SELECT header_hash, height, block FROM full_blocks ORDER BY height")

                block_batch = []

                start_time = time()
                async for r in rows:
                    block = FullBlock.from_bytes(zstd.decompress(r[2]))

                    block_batch.append(block)
                    if len(block_batch) < 32:
                        continue

                    if profile:
                        pr = cProfile.Profile()
                        pr.enable()
                        receive_start_time = time()
                    success, advanced_peak, fork_height, coin_changes = await full_node.receive_block_batch(
                        block_batch, None, None  # type: ignore[arg-type]
                    )
                    if profile:
                        pr.disable()
                        if time() - receive_start_time > 10:
                            pr.create_stats()
                            pr.dump_stats(f"slow-batch-{counter:05d}.profile")

                    assert success
                    assert advanced_peak
                    counter += len(block_batch)
                    print(f"\rheight {counter} {counter/(time() - start_time):0.2f} blocks/s   ", end="")
                    block_batch = []
                    if exit_with_failure:
                        raise RuntimeError("error printed to log. exiting")
        finally:
            print("closing full node")
            full_node._close()
            await full_node._await_closed()


@click.command()
@click.argument("file", type=click.Path(), required=False)
@click.argument("db-version", type=int, required=False, default=2)
@click.option("--profile", is_flag=True, required=False, default=False)
@click.option("--analyze-profiles", is_flag=True, required=False, default=False)
def main(file: Path, db_version: int, profile: bool, analyze_profiles: bool) -> None:
    if analyze_profiles:
        from shlex import quote
        from glob import glob
        from subprocess import check_call

        for input_file in glob("slow-batch-*.profile"):
            output = input_file.replace(".profile", ".png")
            print(f"{input_file}")
            check_call(f"gprof2dot -f pstats {quote(input_file)} | dot -T png >{quote(output)}", shell=True)
        return

    asyncio.run(run_sync_test(Path(file), db_version, profile))


if __name__ == "__main__":
    # pylint: disable = no-value-for-parameter
    main()
