from __future__ import annotations

import click


@click.group()
def main():
    """CLI Commands"""


@main.group()
def yahoo():
    """Commands for Yahoo! Finance"""
