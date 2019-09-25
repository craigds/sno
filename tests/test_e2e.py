import subprocess
import pytest


H = pytest.helpers.helpers()

GPKG_IMPORTS = (
    "archive,gpkg,table_ref",
    [
        pytest.param(
            "gpkg-points", "nz-pa-points-topo-150k.gpkg", "POINTS", id="points"
        )
    ]
)


@pytest.mark.slow
@pytest.mark.e2e
@pytest.mark.parametrize(*GPKG_IMPORTS)
def test_e2e(archive, gpkg, table_ref, data_archive, tmp_path, chdir, cli_runner, geopackage, insert, request):
    table = getattr(H, f"{table_ref}_LAYER")
    row_count = getattr(H, f"{table_ref}_ROWCOUNT")

    repo_path = tmp_path / "myproject.snow"
    repo_path.mkdir()

    remote_path = tmp_path / "myremote.snow"
    remote_path.mkdir()
    with chdir(remote_path):
        # initialise empty repo for remote
        subprocess.run(["git", "init", "--bare", remote_path], check=True)

    with data_archive(archive) as data:
        with chdir(repo_path):
            # initialise empty repo
            r = cli_runner.invoke(["init"])
            assert r.exit_code == 0

            # import data
            r = cli_runner.invoke(["import", "--version=0.2.0", "--x-method=fast", f"GPKG:{data / gpkg}:{table}"])
            assert r.exit_code == 0

            # check there's a commit
            r = cli_runner.invoke(["log"])
            assert r.exit_code == 0
            assert "Import from " in r.stdout
            sha_import = r.stdout.splitlines()[0].split()[1]
            print("Imported SHA:", sha_import)

            # checkout a working copy
            r = cli_runner.invoke(["checkout"])
            assert r.exit_code == 0
            working_copy = (repo_path / "myproject.gpkg")
            assert working_copy.exists()

            # check we have the right data in the WC
            db = geopackage(str(working_copy))
            assert H.row_count(db, table) == row_count

            # create & switch to a new branch
            r = cli_runner.invoke(["switch", "-c", "edit-1"])
            assert r.exit_code == 0
            assert r.stdout.splitlines()[0] == "Creating new branch 'edit-1'..."

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0
            assert r.stdout.splitlines()[0] == "On branch edit-1"

            # make an edit
            insert(db, commit=False)
            r = cli_runner.invoke(["diff"])
            assert r.exit_code == 0
            assert r.stdout.splitlines()[0] == "+++ nz_pa_points_topo_150k:fid=98000"

            # commit it
            r = cli_runner.invoke(["commit", "-m", "commit-1"])
            assert r.exit_code == 0
            sha_edit1 = r.stdout.splitlines()[-1].split()[1]
            print("Edit SHA:", sha_edit1)

            # go back to master
            r = cli_runner.invoke(["switch", "master"])
            assert r.exit_code == 0
            assert r.stdout.splitlines()[0] == f"Updating {working_copy.name} ..."

            r = cli_runner.invoke(["status"])
            assert r.exit_code == 0
            assert r.stdout.splitlines()[0] == "On branch master"

            # merge it
            r = cli_runner.invoke(["merge", "edit-1", "--no-ff"])
            assert r.exit_code == 0
            assert 'Fast-forward' not in r.stdout
            sha_merge1 = r.stdout.splitlines()[-2].split(': ')[1]
            print("Merge SHA:", sha_merge1)

            H.git_graph(request, "post edit-1 merge", count=10)

            # add a remote
            r = cli_runner.invoke(["remote", "add", "myremote", remote_path])
            assert r.exit_code == 0

            # push
            r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
            assert r.exit_code == 0
            assert "Branch 'master' set up to track remote branch 'master' from 'myremote'." in r.stdout