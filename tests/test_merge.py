import json
import pytest

import pygit2

from sno.exceptions import SUCCESS, INVALID_OPERATION, NO_CONFLICT
from sno.merge_util import MergeIndex, CommitWithReference
from sno.repo_files import (
    MERGE_HEAD,
    MERGE_BRANCH,
    MERGE_MSG,
    MERGE_INDEX,
    repo_file_exists,
    read_repo_file,
    RepoState,
)

H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "archive",
    [
        pytest.param("points", id="points"),
        pytest.param("polygons", id="polygons"),
        pytest.param("table", id="table"),
    ],
)
def test_merge_fastforward(
    archive, data_working_copy, geopackage, cli_runner, insert, request
):
    with data_working_copy(archive) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        h = repo.head.target.hex

        # make some changes
        db = geopackage(wc)
        insert(db)
        insert(db)
        commit_id = insert(db)

        H.git_graph(request, "pre-merge")
        assert repo.head.target.hex == commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != commit_id

        r = cli_runner.invoke(["merge", "--ff-only", "changes"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-merge")

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == commit_id
        c = repo.head.peel(pygit2.Commit)
        assert len(c.parents) == 1
        assert c.parents[0].parents[0].parents[0].hex == h


@pytest.mark.parametrize(
    "archive",
    [
        pytest.param("points", id="points"),
        pytest.param("polygons", id="polygons"),
        pytest.param("table", id="table"),
    ],
)
def test_merge_fastforward_noff(
    archive, data_working_copy, geopackage, cli_runner, insert, request
):
    with data_working_copy(archive) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        h = repo.head.target.hex

        # make some changes
        db = geopackage(wc)
        insert(db)
        insert(db)
        commit_id = insert(db)

        H.git_graph(request, "pre-merge")
        assert repo.head.target.hex == commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != commit_id

        # force creation of a merge commit
        r = cli_runner.invoke(["merge", "changes", "--no-ff", "--json"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-merge")

        merge_commit_id = json.loads(r.stdout)["sno.merge/v1"]["commit"]

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == merge_commit_id
        c = repo.head.peel(pygit2.Commit)
        assert len(c.parents) == 2
        assert c.parents[0].hex == h
        assert c.parents[1].hex == commit_id
        assert c.message == 'Merge branch "changes" into master'


@pytest.mark.parametrize(
    "archive,layer,pk_field",
    [
        pytest.param("points", H.POINTS.LAYER, H.POINTS.LAYER_PK, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, H.POLYGONS.LAYER_PK, id="polygons"),
        pytest.param("table", H.TABLE.LAYER, H.TABLE.LAYER_PK, id="table"),
    ],
)
def test_merge_true(
    archive, layer, pk_field, data_working_copy, geopackage, cli_runner, insert, request
):
    with data_working_copy(archive) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        h = repo.head.target.hex

        # make some changes
        db = geopackage(wc)
        dbcur = db.cursor()
        insert(db)
        insert(db)
        b_commit_id = insert(db)
        assert repo.head.target.hex == b_commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != b_commit_id
        m_commit_id = insert(db)
        H.git_graph(request, "pre-merge-master")

        # fastforward merge should fail
        r = cli_runner.invoke(["merge", "--ff-only", "changes"])
        assert r.exit_code == INVALID_OPERATION, r
        assert (
            "Can't resolve as a fast-forward merge and --ff-only specified" in r.stderr
        )

        r = cli_runner.invoke(["merge", "changes", "--ff", "--json"])
        assert r.exit_code == 0, r
        H.git_graph(request, "post-merge")

        merge_commit_id = json.loads(r.stdout)["sno.merge/v1"]["commit"]

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == merge_commit_id
        c = repo.head.peel(pygit2.Commit)
        assert len(c.parents) == 2
        assert c.parents[0].hex == m_commit_id
        assert c.parents[1].hex == b_commit_id
        assert c.parents[0].parents[0].hex == h
        assert c.message == 'Merge branch "changes" into master'

        # check the database state
        num_inserts = len(insert.inserted_fids)
        dbcur.execute(
            f"SELECT COUNT(*) FROM {layer} WHERE {pk_field} IN ({','.join(['?']*num_inserts)});",
            insert.inserted_fids,
        )
        assert dbcur.fetchone()[0] == num_inserts


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(H.POINTS, id="points",),
        pytest.param(H.POLYGONS, id="polygons",),
        pytest.param(H.TABLE, id="table"),
    ],
)
@pytest.mark.parametrize(
    "output_format", ["text", "json"],
)
@pytest.mark.parametrize(
    "dry_run", [pytest.param(False, id=""), pytest.param(True, id="dryrun")],
)
def test_merge_conflicts(
    data, output_format, dry_run, create_conflicts, cli_runner,
):
    with create_conflicts(data) as repo:
        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")

        cmd = ["merge", "theirs_branch", f"--{output_format}"]
        if dry_run:
            cmd += ["--dry-run"]

        r = cli_runner.invoke(cmd)
        assert r.exit_code == 0, r

        if output_format == "text":
            merging_state_message = (
                ["(Not actually merging due to --dry-run)", ""]
                if dry_run
                else [
                    'Repository is now in "merging" state.',
                    "View conflicts with `sno conflicts` and resolve them with `sno resolve`.",
                    "Once no conflicts remain, complete this merge with `sno merge --continue`.",
                    "Or use `sno merge --abort` to return to the previous state.",
                    "",
                ]
            )

            assert (
                r.stdout.split("\n")
                == [
                    'Merging branch "theirs_branch" into ours_branch',
                    "Conflicts found:",
                    "",
                    f"{data.LAYER}:",
                    "  Feature conflicts:",
                    "    add/add: 1",
                    "    edit/edit: 3",
                    "",
                ]
                + merging_state_message
            )

        else:
            jdict = json.loads(r.stdout)
            assert jdict == {
                "sno.merge/v1": {
                    "branch": "ours_branch",
                    "commit": ours.id.hex,
                    "merging": {
                        "ancestor": {
                            "commit": ancestor.id.hex,
                            "abbrevCommit": ancestor.short_id,
                        },
                        "ours": {
                            "branch": "ours_branch",
                            "commit": ours.id.hex,
                            "abbrevCommit": ours.short_id,
                        },
                        "theirs": {
                            "branch": "theirs_branch",
                            "commit": theirs.id.hex,
                            "abbrevCommit": theirs.short_id,
                        },
                    },
                    "dryRun": dry_run,
                    "message": "Merge branch \"theirs_branch\" into ours_branch",
                    "conflicts": {
                        data.LAYER: {
                            "featureConflicts": {"add/add": 1, "edit/edit": 3}
                        },
                    },
                    "state": "merging",
                },
            }

        if not dry_run:
            assert read_repo_file(repo, MERGE_HEAD) == theirs.id.hex + "\n"
            assert read_repo_file(repo, MERGE_BRANCH) == "theirs_branch\n"
            assert (
                read_repo_file(repo, MERGE_MSG)
                == "Merge branch \"theirs_branch\" into ours_branch\n"
            )

            merge_index = MergeIndex.read_from_repo(repo)
            assert len(merge_index.conflicts) == 4
            cli_runner.invoke(["merge", "--abort"])

        assert not repo_file_exists(repo, MERGE_HEAD)
        assert not repo_file_exists(repo, MERGE_BRANCH)
        assert not repo_file_exists(repo, MERGE_MSG)
        assert not repo_file_exists(repo, MERGE_INDEX)


def test_merge_state_lock(create_conflicts, cli_runner):
    with create_conflicts(H.POINTS) as repo:
        # Repo state: normal
        # sno checkout works, but sno conflicts and sno resolve do not.
        assert RepoState.get_state(repo) == RepoState.NORMAL

        r = cli_runner.invoke(["checkout", "ours_branch"])
        assert r.exit_code == SUCCESS
        r = cli_runner.invoke(["conflicts"])
        assert r.exit_code == INVALID_OPERATION
        r = cli_runner.invoke(["resolve", "dummy_conflict", "--delete"])
        assert r.exit_code == INVALID_OPERATION

        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == SUCCESS

        # Repo state: merging
        assert RepoState.get_state(repo) == RepoState.MERGING

        # sno checkout is locked, but sno conflicts and sno resolve work.
        r = cli_runner.invoke(["checkout", "ours_branch"])
        assert r.exit_code == INVALID_OPERATION
        r = cli_runner.invoke(["conflicts"])
        assert r.exit_code == SUCCESS
        r = cli_runner.invoke(["resolve", "dummy_conflict", "--delete"])
        assert r.exit_code == NO_CONFLICT  # "dummy_conflict" is not a real conflict
