import click

from hotel_analyzer.hotel_analyzer_functions import *
from hotel_analyzer.hotel_analyzer_main import hotel_analyzer_main

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option(
    "--data", prompt="Path to archive with data", help="Path to archive with data."
)
@click.option("--output_dir", default="./results", help="Output dir for results.")
@click.option("--size", default=100, help="Size of output csv's")
@click.option("--threads", default=4, help="Number of threads")
def start_hotel_analyzer(data, output_dir, size, threads):
    hotel_analyzer_main(data, output_dir, size, threads)


if __name__ == "__main__":
    start_hotel_analyzer()
