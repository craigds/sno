import collections
import copy
import logging
import re
import sys
from pathlib import Path

import click

from .diff_output import *  # noqa - used from globals()
from .exceptions import (
    InvalidOperation,
    NotFound,
    NO_WORKING_COPY,
    UNCATEGORIZED_ERROR,
)
from .filter_util import build_feature_filter, UNFILTERED
from .repo_files import RepoState
from .structure import RepositoryStructure


L = logging.getLogger("sno.diff")


class Conflict(Exception):
    pass


class Diff:
    def __init__(
        self, dataset_or_diff, meta=None, inserts=None, updates=None, deletes=None
    ):
        # @meta: {}
        # @inserts: [{object}, ...]
        # @deletes: {pk:(oldObject, newObject), ...}
        # @updates: {pk:{object}, ...}
        if dataset_or_diff is None:
            # empty
            self._data = {}
            self._datasets = {}
        elif isinstance(dataset_or_diff, Diff):
            # clone
            diff = dataset_or_diff
            self._data = copy.deepcopy(diff._data)
            self._datasets = copy.copy(diff._datasets)
        else:
            dataset = dataset_or_diff
            self._data = {
                dataset.path: {
                    "META": meta or {},
                    "I": inserts or [],
                    "U": updates or {},
                    "D": deletes or {},
                }
            }
            self._datasets = {dataset.path: dataset}

    def __invert__(self):
        """ Return a new Diff that is the reverse of this Diff """
        new_diff = Diff(self)
        for ds_path, od in new_diff._data.items():
            ds = new_diff._datasets[ds_path]
            if od["META"]:
                raise NotImplementedError(
                    "Can't invert diffs containing meta changes yet"
                )

            new_diff._data[ds_path] = {
                # deletes become inserts
                "I": list(od["D"].values()),
                # inserts become deletes
                "D": {str(o[ds.primary_key]): o for o in od["I"]},
                # updates are swapped old<>new
                "U": {k: (v1, v0) for k, (v0, v1) in od["U"].items()},
                "META": {},
            }
        return new_diff

    def __or__(self, other):
        """
        Return a new Diff with datasets from this Diff and other.
        If a dataset exists in both this Diff and other, a ValueError will be raised
        """
        my_datasets = set(self._data.keys())
        other_datasets = set(other._data.keys())
        if my_datasets & other_datasets:
            raise ValueError(
                f"Same dataset appears in both Diffs, do you want + ? {', '.join(my_datasets & other_datasets)}"
            )

        new_diff = Diff(self)
        new_diff._data.update(copy.deepcopy(other._data))
        new_diff._datasets.update(copy.deepcopy(other._datasets))
        return new_diff

    def __ior__(self, other):
        """
        Update this Diff with datasets from other.
        If a dataset exists in both this Diff and other, a ValueError will be raised
        """
        my_datasets = set(self._datasets.keys())
        other_datasets = set(other._datasets.keys())
        if my_datasets & other_datasets:
            raise ValueError(
                f"Same dataset appears in both Diffs, do you want += ? {', '.join(my_datasets & other_datasets)}"
            )

        self._data.update(copy.deepcopy(other._data))
        self._datasets.update(copy.copy(other._datasets))
        return self

    @classmethod
    def _add(cls, a, b, a_pk, b_pk):

        if any(a["META"].values()) or any(b["META"].values()):
            raise NotImplementedError("Metadata changes")

        conflict_keys = set()

        # we edit both sides during iteration

        a_inserts = {str(o[a_pk]): o for o in a["I"]}
        a_updates = a["U"].copy()
        a_deletes = a["D"].copy()
        L.debug("initial a.inserts: %s", sorted(a_inserts.keys()))
        L.debug("initial a.updates: %s", sorted(a_updates.keys()))
        L.debug("initial a.deletes: %s", sorted(a_deletes.keys()))

        b_inserts = {str(o[b_pk]): o for o in b["I"]}
        b_updates = b["U"].copy()
        b_deletes = b["D"].copy()
        L.debug("initial b.inserts: %s", sorted(b_inserts.keys()))
        L.debug("initial b.updates: %s", sorted(b_updates.keys()))
        L.debug("initial b.deletes: %s", sorted(b_deletes.keys()))

        out_ins = {}
        out_upd = {}
        out_del = {}

        for pk, o in a_inserts.items():
            # ins + ins -> Conflict
            # ins + upd -> ins
            # ins + del -> noop
            # ins +     -> ins

            b_ins = b_inserts.pop(pk, None)
            if b_ins:
                conflict_keys.add(pk)
                continue

            b_upd = b_updates.pop(pk, None)
            if b_upd:
                out_ins[pk] = b_upd[1]
                continue

            b_del = b_deletes.pop(pk, None)
            if b_del:
                continue  # never existed -> noop

            out_ins[pk] = o

        for pk, (a_old, a_new) in a_updates.items():
            # upd + ins -> Conflict
            # upd + upd -> upd?
            # upd + del -> del
            # upd +     -> upd

            b_ins = b_inserts.pop(pk, None)
            if b_ins:
                conflict_keys.add(pk)
                continue

            b_upd = b_updates.pop(pk, None)
            if b_upd:
                b_old, b_new = b_upd
                if a_old != b_new:
                    out_upd[pk] = (a_old, b_new)
                else:
                    pass  # changed back -> noop
                continue

            b_del = b_deletes.pop(pk, None)
            if b_del:
                out_del[pk] = a_old
                continue

            out_upd[pk] = (a_old, a_new)

        for pk, o in a_deletes.items():
            # del + del -> Conflict
            # del + upd -> Conflict
            # del + ins -> upd?
            # del +     -> del

            b_del = b_deletes.pop(pk, None)
            if b_del:
                conflict_keys.add(pk)
                continue

            b_upd = b_updates.pop(pk, None)
            if b_upd:
                conflict_keys.add(pk)
                continue

            b_ins = b_inserts.pop(pk, None)
            if b_ins:
                if b_ins != o:
                    out_upd[pk] = (o, b_ins)
                else:
                    pass  # inserted same as deleted -> noop
                continue

            out_del[pk] = o

        # we should only have keys left in b.* that weren't in a.*
        L.debug("out_ins: %s", sorted(out_ins.keys()))
        L.debug("out_upd: %s", sorted(out_upd.keys()))
        L.debug("out_del: %s", sorted(out_del.keys()))
        L.debug("remaining b.inserts: %s", sorted(b_inserts.keys()))
        L.debug("remaining b.updates: %s", sorted(b_updates.keys()))
        L.debug("remaining b.deletes: %s", sorted(b_deletes.keys()))

        all_keys = sum(
            [
                list(l)
                for l in [
                    out_ins.keys(),
                    out_upd.keys(),
                    out_del.keys(),
                    b_inserts.keys(),
                    b_updates.keys(),
                    b_deletes.keys(),
                ]
            ],
            [],
        )
        e = set(all_keys)
        if len(e) != len(all_keys):
            e_keys = [
                k for k, count in collections.Counter(all_keys).items() if count > 1
            ]
            raise AssertionError(
                f"Unexpected key conflict between operations: {e_keys}"
            )

        #     + ins -> ins
        #     + upd -> upd
        #     + del -> del
        out_ins.update(b_inserts)
        out_upd.update(b_updates)
        out_del.update(b_deletes)

        return (
            {
                "META": {},
                "I": sorted(out_ins.values(), key=lambda o: o[b_pk]),
                "U": out_upd,
                "D": out_del,
            },
            conflict_keys or None,
        )

    def __add__(self, other):
        my_datasets = set(self._data.keys())
        other_datasets = set(other._data.keys())

        new_diff = Diff(self)
        for ds in other_datasets:
            if ds not in my_datasets:
                new_diff._data[ds] = other._data[ds]
                new_diff._datasets[ds] = other._datasets[ds]
            else:
                rdiff, conflicts = self._add(
                    a=self._data[ds],
                    b=other._data[ds],
                    a_pk=self._datasets[ds].primary_key,
                    b_pk=other._datasets[ds].primary_key,
                )
                if conflicts:
                    raise Conflict(conflicts)
                else:
                    new_diff._data[ds] = rdiff
        return new_diff

    def __iadd__(self, other):
        my_datasets = set(self._data.keys())
        other_datasets = set(other._data.keys())

        for ds in other_datasets:
            if ds not in my_datasets:
                self._data[ds] = other._data[ds]
                self._datasets[ds] = other._datasets[ds]
            else:
                rdiff, conflicts = self._add(
                    a=self._data[ds],
                    b=other._data[ds],
                    a_pk=self._datasets[ds].primary_key,
                    b_pk=other._datasets[ds].primary_key,
                )
                if conflicts:
                    raise Conflict(conflicts)
                else:
                    self._data[ds] = rdiff
        return self

    def __len__(self):
        count = 0
        for dataset_diff in self._data.values():
            count += sum(len(o) for o in dataset_diff.values())
        return count

    def __getitem__(self, dataset):
        if isinstance(dataset, str):
            return self._data[dataset]
        return self._data[dataset.path]

    def __iter__(self):
        for ds_path, dsdiff in self._data.items():
            ds = self._datasets[ds_path]
            yield ds, dsdiff

    def __eq__(self, other):
        if set(self._datasets.keys()) != set(other._datasets.keys()):
            return False

        for ds, sdiff in self:
            odiff = other[ds]
            if sorted(sdiff["I"], key=lambda o: o[ds.primary_key]) != sorted(
                odiff["I"], key=lambda o: o[ds.primary_key]
            ):
                return False
            if sdiff["META"] != odiff["META"]:
                return False
            if sdiff["U"] != odiff["U"]:
                return False
            if sdiff["D"] != odiff["D"]:
                return False

        return True

    def dataset_counts(self, dataset):
        """Returns a dict containing the count of each type of diff, for a particular dataset."""
        return {k: len(v) for k, v in self._data[dataset.path].items()}

    def counts(self):
        """
        Returns multiple dataset_counts dicts, one for each dataset touched by this diff.
        The dataset_counts dicts are returned in a top-level dict keyed by dataset path.
        """

        return {
            dataset.path: self.dataset_counts(dataset) for dataset in self.datasets()
        }

    def __repr__(self):
        return repr(self._data)

    def datasets(self):
        return self._datasets.values()

    def to_filter(self):
        """
        Returns a filter object - see filter_util.py - that matches all features affected by this diff.
        """
        return {dataset.path: self._dataset_pks(dataset) for dataset in self.datasets()}

    def _dataset_pks(self, dataset):
        """Returns the set of all pks affected by this diff within a particular dataset."""
        ds_data = self._data[dataset.path]
        primary_key = dataset.primary_key
        inserts = [str(o[primary_key]) for o in ds_data["I"]]
        deletes = [str(o[primary_key]) for o in ds_data["D"].values()]
        update_olds = [str(o[0][primary_key]) for o in ds_data["U"].values()]
        update_news = [str(o[1][primary_key]) for o in ds_data["U"].values()]
        return set(inserts + deletes + update_olds + update_news)


def get_dataset_diff(
    base_rs, target_rs, working_copy, dataset_path, pk_filter=UNFILTERED
):
    dataset = base_rs.get(dataset_path) or target_rs.get(dataset_path)
    diff = Diff(dataset)

    if base_rs != target_rs:
        # diff += base_rs<>target_rs
        base_ds = base_rs.get(dataset_path)
        target_ds = target_rs.get(dataset_path)

        params = {}
        if not base_ds:
            base_ds, target_ds = target_ds, base_ds
            params["reverse"] = True

        diff_cc = base_ds.diff(target_ds, pk_filter=pk_filter, **params)
        L.debug("commit<>commit diff (%s): %s", dataset_path, repr(diff_cc))
        diff += diff_cc

    if working_copy:
        # diff += target_rs<>working_copy
        target_ds = target_rs.get(dataset_path)
        diff_wc = working_copy.diff_db_to_tree(target_ds, pk_filter=pk_filter)
        L.debug(
            "commit<>working_copy diff (%s): %s", dataset_path, repr(diff_wc),
        )
        diff += diff_wc

    return diff


def get_repo_diff(base_rs, target_rs, feature_filter=UNFILTERED):
    """Generates a Diff for every dataset in both RepositoryStructures."""
    all_datasets = {ds.path for ds in base_rs} | {ds.path for ds in target_rs}

    if feature_filter is not UNFILTERED:
        all_datasets = all_datasets.intersection(feature_filter.keys())

    result = Diff(None)
    for dataset in all_datasets:
        result += get_dataset_diff(
            base_rs, target_rs, None, dataset, feature_filter[dataset]
        )
    return result


def get_common_ancestor(repo, rs1, rs2):
    for rs in rs1, rs2:
        if not rs.head_commit:
            raise click.UsageError(
                f"The .. operator works on commits, not trees - {rs.id} is a tree. (Perhaps try the ... operator)"
            )
    ancestor_id = repo.merge_base(rs1.id, rs2.id)
    if not ancestor_id:
        raise InvalidOperation(
            "The .. operator tries to find the common ancestor, but no common ancestor was found. Perhaps try the ... operator."
        )
    return RepositoryStructure.lookup(repo, ancestor_id)


def diff_with_writer(
    ctx,
    diff_writer,
    *,
    output_path='-',
    exit_code,
    json_style="pretty",
    commit_spec,
    filters,
):
    """
    Calculates the appropriate diff from the arguments,
    and writes it using the given writer contextmanager.

      ctx: the click context
      diff_writer: One of the `diff_output_*` contextmanager factories.
                   When used as a contextmanager, the diff_writer should yield
                   another callable which accepts (dataset, diff) arguments
                   and writes the output by the time it exits.
      output_path: The output path, or a file-like object, or the string '-' to use stdout.
      exit_code:   If True, the process will exit with code 1 if the diff is non-empty.
      commit_spec: The commit-ref or -refs to diff.
      filters:     Limit the diff to certain datasets or features.
    """
    from .working_copy import WorkingCopy

    try:
        if isinstance(output_path, str) and output_path != "-":
            output_path = Path(output_path).expanduser()

        repo = ctx.obj.get_repo(allowed_states=RepoState.ALL_STATES)

        # Parse <commit> or <commit>...<commit>
        commit_spec = commit_spec or "HEAD"
        commit_parts = re.split(r"(\.{2,3})", commit_spec)

        if len(commit_parts) == 3:
            # Two commits specified - base and target. We diff base<>target.
            base_rs = RepositoryStructure.lookup(repo, commit_parts[0] or "HEAD")
            target_rs = RepositoryStructure.lookup(repo, commit_parts[2] or "HEAD")
            if commit_parts[1] == "..":
                # A   C    A...C is A<>C
                #  \ /     A..C  is B<>C
                #   B      (git log semantics)
                base_rs = get_common_ancestor(repo, base_rs, target_rs)
            working_copy = None
        else:
            # When one commit is specified, it is base, and we diff base<>working_copy.
            # When no commits are specified, base is HEAD, and we do the same.
            # We diff base<>working_copy by diffing base<>target + target<>working_copy,
            # and target is set to HEAD.
            base_rs = RepositoryStructure.lookup(repo, commit_parts[0])
            target_rs = RepositoryStructure.lookup(repo, "HEAD")
            working_copy = WorkingCopy.open(repo)
            if not working_copy:
                raise NotFound(
                    "No working copy, use 'checkout'", exit_code=NO_WORKING_COPY
                )
            working_copy.assert_db_tree_match(target_rs.tree)

        # Parse [<dataset>[:pk]...]
        feature_filter = build_feature_filter(filters)

        base_str = base_rs.id
        target_str = "working-copy" if working_copy else target_rs.id
        L.debug('base=%s target=%s', base_str, target_str)

        all_datasets = {ds.path for ds in base_rs} | {ds.path for ds in target_rs}

        if feature_filter is not UNFILTERED:
            all_datasets = all_datasets.intersection(feature_filter.keys())

        writer_params = {
            "repo": repo,
            "base": base_rs,
            "target": target_rs,
            "output_path": output_path,
            "dataset_count": len(all_datasets),
            "json_style": json_style,
        }

        L.debug(
            "base_rs %s == target_rs %s: %s",
            repr(base_rs),
            repr(target_rs),
            base_rs == target_rs,
        )

        num_changes = 0
        with diff_writer(**writer_params) as w:
            for dataset_path in all_datasets:
                diff = get_dataset_diff(
                    base_rs,
                    target_rs,
                    working_copy,
                    dataset_path,
                    feature_filter[dataset_path],
                )
                [dataset] = diff.datasets()
                num_changes += len(diff)
                L.debug("overall diff (%s): %s", dataset_path, repr(diff))
                w(dataset, diff[dataset])

    except click.ClickException as e:
        L.debug("Caught ClickException: %s", e)
        if exit_code and e.exit_code == 1:
            e.exit_code = UNCATEGORIZED_ERROR
        raise
    except Exception as e:
        L.debug("Caught non-ClickException: %s", e)
        if exit_code:
            click.secho(f"Error: {e}", fg="red", file=sys.stderr)
            raise SystemExit(UNCATEGORIZED_ERROR) from e
        else:
            raise
    else:
        if exit_code and num_changes:
            sys.exit(1)


@click.command()
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json", "geojson", "quiet", "html"]),
    default="text",
    help=(
        "Output format. 'quiet' disables all output and implies --exit-code.\n"
        "'html' attempts to open a browser unless writing to stdout ( --output=- )"
    ),
)
@click.option(
    "--exit-code",
    is_flag=True,
    help="Make the program exit with codes similar to diff(1). That is, it exits with 1 if there were differences and 0 means no differences.",
)
@click.option(
    "--output",
    "output_path",
    help="Output to a specific file/directory instead of stdout.",
    type=click.Path(writable=True, allow_dash=True),
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the output. Only used with -o json or -o geojson",
)
@click.argument("commit_spec", required=False, nargs=1)
@click.argument("filters", nargs=-1)
def diff(ctx, output_format, output_path, exit_code, json_style, commit_spec, filters):
    """
    Show changes between two commits, or between a commit and the working copy.

    COMMIT_SPEC -

    - if not supplied, the default is HEAD, to diff between HEAD and the working copy.

    - if a single ref is supplied: commit-A - diffs between commit-A and the working copy.

    - if supplied with the form: commit-A...commit-B - diffs between commit-A and commit-B.

    - if supplied with the form: commit-A..commit-B - diffs between (the common ancestor of
    commit-A and commit-B) and (commit-B).

    To list only particular conflicts, supply one or more FILTERS of the form [DATASET[:PRIMARY_KEY]]
    """

    diff_writer = globals()[f"diff_output_{output_format}"]
    if output_format == "quiet":
        exit_code = True

    return diff_with_writer(
        ctx,
        diff_writer,
        output_path=output_path,
        exit_code=exit_code,
        json_style=json_style,
        commit_spec=commit_spec,
        filters=filters,
    )
