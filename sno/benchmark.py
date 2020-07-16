from contextlib import contextmanager
import cProfile
import io
import pstats
import subprocess

import click
import pygit2

from . import core


@click.group(chain=True)
@click.pass_context
def benchmark(ctx, **kwargs):
    """
    Various benchmarks
    """
    ...


def _get_blob_paths(tree):
    paths = []
    for (top, path, subtree_names, blob_names) in core.walk_tree(tree):
        paths.extend(f'{path}/{b}' for b in blob_names)
    return paths


def _get_blobs(tree):
    """
    Returns a list of blobs under the given tree.
    """
    blobs = []
    for (top, path, subtree_names, blob_names) in core.walk_tree(tree):
        blobs.extend(top / b for b in blob_names)
    return blobs


@contextmanager
def _profile():
    pr = cProfile.Profile()
    print("[starting profiler]")
    pr.enable()
    yield
    pr.disable()
    print("[stopped profiler]")
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats()
    print(s.getvalue())


@benchmark.command(name='empty-string')
@click.pass_context
def empty_string(ctx):
    """
    A placebo
    """
    tree = ctx.obj.repo.head.peel(pygit2.Tree)
    blobs = _get_blobs(tree)

    def _get_data(blob):
        return ''

    with _profile():
        for blob in blobs:
            _get_data(blob)


@benchmark.command(name='blob-data')
@click.pass_context
def blob_data(ctx):
    tree = ctx.obj.repo.head.peel(pygit2.Tree)
    blobs = _get_blobs(tree)

    def _get_data(blob):
        return blob.data

    with _profile():
        for blob in blobs:
            _get_data(blob)


@benchmark.command(name='blob-memoryview')
@click.pass_context
def blob_memoryview(ctx):
    tree = ctx.obj.repo.head.peel(pygit2.Tree)
    blobs = _get_blobs(tree)

    def _get_data(blob):
        return memoryview(blob)

    with _profile():
        for blob in blobs:
            _get_data(blob)


@benchmark.command(name='cat-file')
@click.pass_context
def cat_file(ctx):
    tree = ctx.obj.repo.head.peel(pygit2.Tree)
    ids = [str(blob.id) for blob in _get_blobs(tree)]

    def _get_data(id):
        return subprocess.check_output(['git', 'cat-file', '-p', id])

    with _profile():
        for id in ids:
            _get_data(id)


@benchmark.command(name='cat-file-batch')
@click.pass_context
def cat_file_batch(ctx):
    tree = ctx.obj.repo.head.peel(pygit2.Tree)
    ids_bytes = '\n'.join(str(blob.id) for blob in _get_blobs(tree)).encode('ascii')

    with _profile():
        subprocess.run(
            ['git', 'cat-file', '--batch=%(objecttype)'],
            input=ids_bytes,
            capture_output=False,
            stdout=subprocess.DEVNULL,
        )


@benchmark.command(name='cat-file-batch-all')
@click.pass_context
def cat_file_batch_all(ctx):
    with _profile():
        subprocess.run(
            ['git', 'cat-file', '--batch-all-objects', '--batch=%(objecttype)'],
            capture_output=False,
            stdout=subprocess.DEVNULL,
        )


@benchmark.command(name='cat-file-batch-all-unordered')
@click.pass_context
def cat_file_batch_all_unordered(ctx):
    with _profile():
        subprocess.run(
            [
                'git',
                'cat-file',
                '--batch-all-objects',
                '--batch=%(objecttype)',
                '--unordered',
            ],
            capture_output=False,
            stdout=subprocess.DEVNULL,
        )


@benchmark.command(name='all')
@click.pass_context
def do_all(ctx):
    commands = (
        blob_data,
        blob_memoryview,
        cat_file_batch,
        cat_file_batch_all,
        cat_file_batch_all_unordered,
        # this one's by far the slowest, do it last
        cat_file,
    )
    for c in commands:
        click.secho(c.name, bold=True)
        ctx.invoke(c)
