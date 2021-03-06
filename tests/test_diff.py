import collections
import json
import re

import html5lib
import pytest

import pygit2
from sno.diff import Diff


H = pytest.helpers.helpers()

DIFF_OUTPUT_FORMATS = ["text", "geojson", "json", "quiet", "html"]
SHOW_OUTPUT_FORMATS = ["text", "json"]
V1_OR_V2 = ("structure_version", ["1", "2"])


def _check_html_output(s):
    parser = html5lib.HTMLParser(strict=True, namespaceHTMLElements=False)
    # throw errors on invalid HTML
    document = parser.parse(s)
    # find the <script> element containing data
    el = document.find("./head/script[@id='sno-data']")
    # find the JSON
    m = re.match(r"\s*const DATA=(.*);\s*$", el.text, flags=re.DOTALL)
    # validate it
    json.loads(m.group(1))


@pytest.mark.parametrize("output_format", DIFF_OUTPUT_FORMATS)
@pytest.mark.parametrize(*V1_OR_V2)
def test_diff_points(
    structure_version, output_format, data_working_copy, geopackage, cli_runner
):
    """ diff the working copy against HEAD """
    data_archive = "points2" if structure_version == "2" else "points"
    with data_working_copy(data_archive) as (repo, wc):
        # empty
        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-", "--exit-code"]
        )
        assert r.exit_code == 0, r

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()

            cur.execute(H.POINTS.INSERT, H.POINTS.RECORD)
            assert db.changes() == 1
            cur.execute(f"UPDATE {H.POINTS.LAYER} SET fid=9998 WHERE fid=1;")
            assert db.changes() == 1
            cur.execute(
                f"UPDATE {H.POINTS.LAYER} SET name='test', t50_fid=NULL WHERE fid=2;"
            )
            assert db.changes() == 1
            cur.execute(f"DELETE FROM {H.POINTS.LAYER} WHERE fid=3;")
            assert db.changes() == 1

        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-"]
        )
        print("STDOUT", repr(r.stdout))
        if output_format == "quiet":
            assert r.exit_code == 1, r
            assert r.stdout == ""
        elif output_format == "text":
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                "--- nz_pa_points_topo_150k:fid=3",
                "-                                     geom = POINT(...)",
                "-                               macronated = N",
                "-                                     name = Tauwhare Pa",
                "-                               name_ascii = Tauwhare Pa",
                "-                                  t50_fid = 2426273",
                "+++ nz_pa_points_topo_150k:fid=9999",
                "+                                     geom = POINT(...)",
                "+                               macronated = 0",
                "+                                     name = Te Motu-a-kore",
                "+                               name_ascii = Te Motu-a-kore",
                "+                                  t50_fid = 9999999",
                "--- nz_pa_points_topo_150k:fid=2",
                "+++ nz_pa_points_topo_150k:fid=2",
                "-                                     name = ␀",
                "+                                     name = test",
                "-                                  t50_fid = 2426272",
                "+                                  t50_fid = ␀",
                "--- nz_pa_points_topo_150k:fid=1",
                "+++ nz_pa_points_topo_150k:fid=9998",
            ]
        elif output_format == "geojson":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert len(odata["features"]) == 6
            assert odata == {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.071_252_196_287_02,
                                -37.979_475_484_627_57,
                            ],
                        },
                        "properties": {
                            "fid": 3,
                            "macronated": "N",
                            "name": "Tauwhare Pa",
                            "name_ascii": "Tauwhare Pa",
                            "t50_fid": 2_426_273,
                        },
                        "id": "D::3",
                    },
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [0.0, 0.0]},
                        "properties": {
                            "fid": 9999,
                            "t50_fid": 9_999_999,
                            "name_ascii": "Te Motu-a-kore",
                            "macronated": "0",
                            "name": "Te Motu-a-kore",
                        },
                        "id": "I::9999",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.078_662_844_395_9,
                                -37.988_184_857_601_8,
                            ],
                        },
                        "properties": {
                            "fid": 2,
                            "macronated": "N",
                            "name": None,
                            "name_ascii": None,
                            "t50_fid": 2_426_272,
                        },
                        "id": "U-::2",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.078_662_844_395_9,
                                -37.988_184_857_601_8,
                            ],
                        },
                        "properties": {
                            "fid": 2,
                            "t50_fid": None,
                            "name_ascii": None,
                            "macronated": "N",
                            "name": "test",
                        },
                        "id": "U+::2",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.095_962_971_358_6,
                                -38.004_338_036_217_68,
                            ],
                        },
                        "properties": {
                            "fid": 1,
                            "macronated": "N",
                            "name": None,
                            "name_ascii": None,
                            "t50_fid": 2_426_271,
                        },
                        "id": "U-::1",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [
                                177.095_962_971_358_6,
                                -38.004_338_036_217_68,
                            ],
                        },
                        "properties": {
                            "fid": 9998,
                            "t50_fid": 2_426_271,
                            "name_ascii": None,
                            "macronated": "N",
                            "name": None,
                        },
                        "id": "U+::9998",
                    },
                ],
            }
        elif output_format == "json":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert (
                len(
                    odata["sno.diff/v1+hexwkb"]["nz_pa_points_topo_150k"][
                        "featureChanges"
                    ]
                )
                == 4
            )
            assert odata == {
                'sno.diff/v1+hexwkb': {
                    'nz_pa_points_topo_150k': {
                        'featureChanges': [
                            {
                                '+': {
                                    'fid': 9998,
                                    'geom': '010100000097F3EF201223664087D715268E0043C0',
                                    'macronated': 'N',
                                    'name': None,
                                    'name_ascii': None,
                                    't50_fid': 2426271,
                                },
                                '-': {
                                    'fid': 1,
                                    'geom': '010100000097F3EF201223664087D715268E0043C0',
                                    'macronated': 'N',
                                    'name': None,
                                    'name_ascii': None,
                                    't50_fid': 2426271,
                                },
                            },
                            {
                                '+': {
                                    'fid': 2,
                                    'geom': '0101000000E702F16784226640ADE666D77CFE42C0',
                                    'macronated': 'N',
                                    'name': 'test',
                                    'name_ascii': None,
                                    't50_fid': None,
                                },
                                '-': {
                                    'fid': 2,
                                    'geom': '0101000000E702F16784226640ADE666D77CFE42C0',
                                    'macronated': 'N',
                                    'name': None,
                                    'name_ascii': None,
                                    't50_fid': 2426272,
                                },
                            },
                            {
                                '-': {
                                    'fid': 3,
                                    'geom': '0101000000459AAFB247226640C6DAE2735FFD42C0',
                                    'macronated': 'N',
                                    'name': 'Tauwhare Pa',
                                    'name_ascii': 'Tauwhare Pa',
                                    't50_fid': 2426273,
                                }
                            },
                            {
                                '+': {
                                    'fid': 9999,
                                    'geom': '010100000000000000000000000000000000000000',
                                    'macronated': '0',
                                    'name': 'Te Motu-a-kore',
                                    'name_ascii': 'Te Motu-a-kore',
                                    't50_fid': 9999999,
                                }
                            },
                        ],
                        'metaChanges': {},
                    }
                }
            }
        elif output_format == "html":
            _check_html_output(r.stdout)


@pytest.mark.parametrize("output_format", DIFF_OUTPUT_FORMATS)
@pytest.mark.parametrize(*V1_OR_V2)
def test_diff_polygons(
    structure_version, output_format, data_working_copy, geopackage, cli_runner
):
    """ diff the working copy against HEAD """
    data_archive = "polygons2" if structure_version == "2" else "polygons"
    with data_working_copy(data_archive) as (repo, wc):
        # empty
        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-", "--exit-code"]
        )
        assert r.exit_code == 0, r

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()

            cur.execute(H.POLYGONS.INSERT, H.POLYGONS.RECORD)
            assert db.changes() == 1
            cur.execute(f"UPDATE {H.POLYGONS.LAYER} SET id=9998 WHERE id=1424927;")
            assert db.changes() == 1
            cur.execute(
                f"UPDATE {H.POLYGONS.LAYER} SET survey_reference='test', date_adjusted='2019-01-01T00:00:00Z' WHERE id=1443053;"
            )
            assert db.changes() == 1
            cur.execute(f"DELETE FROM {H.POLYGONS.LAYER} WHERE id=1452332;")
            assert db.changes() == 1

        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-"]
        )
        if output_format == "quiet":
            assert r.exit_code == 1, r
            assert r.stdout == ""
        elif output_format == "text":
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                "--- nz_waca_adjustments:id=1452332",
                "-                           adjusted_nodes = 558",
                "-                            date_adjusted = 2011-06-07T15:22:58Z",
                "-                                     geom = MULTIPOLYGON(...)",
                "-                         survey_reference = ␀",
                "+++ nz_waca_adjustments:id=9999999",
                "+                           adjusted_nodes = 123",
                "+                            date_adjusted = 2019-07-05T13:04:00+01:00",
                "+                                     geom = POLYGON(...)",
                "+                         survey_reference = Null Island™ 🗺",
                "--- nz_waca_adjustments:id=1443053",
                "+++ nz_waca_adjustments:id=1443053",
                "-                            date_adjusted = 2011-05-10T12:09:10Z",
                "+                            date_adjusted = 2019-01-01T00:00:00Z",
                "-                         survey_reference = ␀",
                "+                         survey_reference = test",
                "--- nz_waca_adjustments:id=1424927",
                "+++ nz_waca_adjustments:id=9998",
            ]
        elif output_format == "geojson":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert len(odata["features"]) == 6
            assert odata == {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [174.731_157_683_3, -36.799_283_85],
                                        [174.730_470_716_7, -36.796_400_95],
                                        [174.730_472_2, -36.796_323_283_3],
                                        [174.731_246_833_3, -36.795_535_566_7],
                                        [174.731_796_216_7, -36.795_137_983_3],
                                        [174.731_870_233_3, -36.795_087_966_7],
                                        [174.731_899_816_7, -36.795_070_716_7],
                                        [174.732_051_85, -36.794_982_083_3],
                                        [174.732_203_9, -36.794_893_45],
                                        [174.732_812_133_3, -36.794_538_9],
                                        [174.733_139_883_3, -36.794_347_85],
                                        [174.733_307_983_3, -36.794_249_866_7],
                                        [174.733_341_716_7, -36.794_231_666_7],
                                        [174.733_702_1, -36.794_166_5],
                                        [174.733_990_683_3, -36.794_262_933_3],
                                        [174.734_288_05, -36.794_433_116_7],
                                        [174.736_541_133_3, -36.796_472_616_7],
                                        [174.736_568_65, -36.796_552_566_7],
                                        [174.736_553_833_3, -36.796_667],
                                        [174.736_335_3, -36.796_878_85],
                                        [174.736_180_016_7, -36.797_001_816_7],
                                        [174.732_969_516_7, -36.799_071_45],
                                        [174.732_654_483_3, -36.799_214_2],
                                        [174.731_157_683_3, -36.799_283_85],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1_452_332,
                            "adjusted_nodes": 558,
                            "date_adjusted": "2011-06-07T15:22:58Z",
                            "survey_reference": None,
                        },
                        "id": "D::1452332",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [0.0, 0.0],
                                    [0.0, 0.001],
                                    [0.001, 0.001],
                                    [0.001, 0.0],
                                    [0.0, 0.0],
                                ]
                            ],
                        },
                        "properties": {
                            "id": 9_999_999,
                            "date_adjusted": "2019-07-05T13:04:00+01:00",
                            "survey_reference": "Null Island™ 🗺",
                            "adjusted_nodes": 123,
                        },
                        "id": "I::9999999",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [174.216_618_083_3, -39.116_006_916_7],
                                        [174.210_532_433_3, -39.062_889_633_3],
                                        [174.218_767_133_3, -39.044_481_366_7],
                                        [174.233_628_6, -39.043_576_183_3],
                                        [174.248_983_433_3, -39.067_347_716_7],
                                        [174.237_115_083_3, -39.104_299_8],
                                        [174.237_047_966_7, -39.104_386_5],
                                        [174.223_032_466_7, -39.114_993_95],
                                        [174.222_116_8, -39.115_347_05],
                                        [174.219_978_466_7, -39.115_833_983_3],
                                        [174.216_618_083_3, -39.116_006_916_7],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1_443_053,
                            "adjusted_nodes": 1238,
                            "date_adjusted": "2011-05-10T12:09:10Z",
                            "survey_reference": None,
                        },
                        "id": "U-::1443053",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [174.216_618_083_3, -39.116_006_916_7],
                                        [174.210_532_433_3, -39.062_889_633_3],
                                        [174.218_767_133_3, -39.044_481_366_7],
                                        [174.233_628_6, -39.043_576_183_3],
                                        [174.248_983_433_3, -39.067_347_716_7],
                                        [174.237_115_083_3, -39.104_299_8],
                                        [174.237_047_966_7, -39.104_386_5],
                                        [174.223_032_466_7, -39.114_993_95],
                                        [174.222_116_8, -39.115_347_05],
                                        [174.219_978_466_7, -39.115_833_983_3],
                                        [174.216_618_083_3, -39.116_006_916_7],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1_443_053,
                            "date_adjusted": "2019-01-01T00:00:00Z",
                            "survey_reference": "test",
                            "adjusted_nodes": 1238,
                        },
                        "id": "U+::1443053",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [175.365_019_55, -37.867_737_133_3],
                                        [175.359_424_816_7, -37.859_677_466_7],
                                        [175.358_776_6, -37.858_739_4],
                                        [175.357_528_166_7, -37.856_829_966_7],
                                        [175.357_346_8, -37.856_404_883_3],
                                        [175.350_319_216_7, -37.838_409_016_7],
                                        [175.351_635_8, -37.834_856_583_3],
                                        [175.357_739_316_7, -37.827_765_216_7],
                                        [175.358_196_366_7, -37.827_312_183_3],
                                        [175.361_308_266_7, -37.827_064_966_7],
                                        [175.384_347_033_3, -37.849_134_05],
                                        [175.384_300_45, -37.849_304_583_3],
                                        [175.377_467_833_3, -37.860_278_266_7],
                                        [175.375_013_566_7, -37.864_152_2],
                                        [175.373_939_666_7, -37.865_846_683_3],
                                        [175.372_695_366_7, -37.867_499_533_3],
                                        [175.372_516_333_3, -37.867_591_25],
                                        [175.365_019_55, -37.867_737_133_3],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 1_424_927,
                            "adjusted_nodes": 1122,
                            "date_adjusted": "2011-03-25T07:30:45Z",
                            "survey_reference": None,
                        },
                        "id": "U-::1424927",
                    },
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "MultiPolygon",
                            "coordinates": [
                                [
                                    [
                                        [175.365_019_55, -37.867_737_133_3],
                                        [175.359_424_816_7, -37.859_677_466_7],
                                        [175.358_776_6, -37.858_739_4],
                                        [175.357_528_166_7, -37.856_829_966_7],
                                        [175.357_346_8, -37.856_404_883_3],
                                        [175.350_319_216_7, -37.838_409_016_7],
                                        [175.351_635_8, -37.834_856_583_3],
                                        [175.357_739_316_7, -37.827_765_216_7],
                                        [175.358_196_366_7, -37.827_312_183_3],
                                        [175.361_308_266_7, -37.827_064_966_7],
                                        [175.384_347_033_3, -37.849_134_05],
                                        [175.384_300_45, -37.849_304_583_3],
                                        [175.377_467_833_3, -37.860_278_266_7],
                                        [175.375_013_566_7, -37.864_152_2],
                                        [175.373_939_666_7, -37.865_846_683_3],
                                        [175.372_695_366_7, -37.867_499_533_3],
                                        [175.372_516_333_3, -37.867_591_25],
                                        [175.365_019_55, -37.867_737_133_3],
                                    ]
                                ]
                            ],
                        },
                        "properties": {
                            "id": 9998,
                            "date_adjusted": "2011-03-25T07:30:45Z",
                            "survey_reference": None,
                            "adjusted_nodes": 1122,
                        },
                        "id": "U+::9998",
                    },
                ],
            }
        elif output_format == "json":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert (
                len(
                    odata["sno.diff/v1+hexwkb"]["nz_waca_adjustments"]["featureChanges"]
                )
                == 4
            )
            assert odata == {
                'sno.diff/v1+hexwkb': {
                    'nz_waca_adjustments': {
                        'featureChanges': [
                            {
                                '+': {
                                    'adjusted_nodes': 1122,
                                    'date_adjusted': '2011-03-25T07:30:45Z',
                                    'geom': '01060000000100000001030000000100000012000000D2B47A3DAEEB65402E86A80212EF42C01D23796880EB6540D54A46E909EE42C03E7210197BEB6540B164332CEBED42C003ECE8DE70EB6540C99AB69AACED42C0916A8E626FEB654040F4DAAC9EED42C0615CA5D035EB6540F2B295FC50EB42C04AA3B89940EB6540D90F9D94DCEA42C00937B99972EB6540163FEB35F4E942C0B9103A5876EB65408D6D995DE5E942C008A85AD68FEB654069D2CB43DDE942C0D24A26924CEC6540C455AF6CB0EC42C0D21275304CEC6540E6CE3803B6EC42C018EA6B3714EC6540D17726991DEE42C00D91731C00EC65401BE20E8A9CEE42C0EBE45150F7EB6540D10F6A10D4EE42C01C6BD51EEDEB6540CD6886390AEF42C0FB975FA7EBEB6540DB85E63A0DEF42C0D2B47A3DAEEB65402E86A80212EF42C0',
                                    'id': 9998,
                                    'survey_reference': None,
                                },
                                '-': {
                                    'adjusted_nodes': 1122,
                                    'date_adjusted': '2011-03-25T07:30:45Z',
                                    'geom': '01060000000100000001030000000100000012000000D2B47A3DAEEB65402E86A80212EF42C01D23796880EB6540D54A46E909EE42C03E7210197BEB6540B164332CEBED42C003ECE8DE70EB6540C99AB69AACED42C0916A8E626FEB654040F4DAAC9EED42C0615CA5D035EB6540F2B295FC50EB42C04AA3B89940EB6540D90F9D94DCEA42C00937B99972EB6540163FEB35F4E942C0B9103A5876EB65408D6D995DE5E942C008A85AD68FEB654069D2CB43DDE942C0D24A26924CEC6540C455AF6CB0EC42C0D21275304CEC6540E6CE3803B6EC42C018EA6B3714EC6540D17726991DEE42C00D91731C00EC65401BE20E8A9CEE42C0EBE45150F7EB6540D10F6A10D4EE42C01C6BD51EEDEB6540CD6886390AEF42C0FB975FA7EBEB6540DB85E63A0DEF42C0D2B47A3DAEEB65402E86A80212EF42C0',
                                    'id': 1424927,
                                    'survey_reference': None,
                                },
                            },
                            {
                                '+': {
                                    'adjusted_nodes': 1238,
                                    'date_adjusted': '2019-01-01T00:00:00Z',
                                    'geom': '0106000000010000000103000000010000000B000000DDEF0B89EEC665400CAB8C50D98E43C0AA7883AEBCC66540F6237BC40C8843C0D25EEE2300C7654002A1BF90B18543C0218DAFE279C76540391485E7938543C09EE81AACF7C76540E85798D99E8843C02E055F7296C765405BFD22B2598D43C0EA119EE595C765406BD26D895C8D43C087CDFB1423C76540723E2B1FB88E43C08DFCB0941BC7654054B82FB1C38E43C0A00948100AC76540FB04E1A5D38E43C0DDEF0B89EEC665400CAB8C50D98E43C0',
                                    'id': 1443053,
                                    'survey_reference': 'test',
                                },
                                '-': {
                                    'adjusted_nodes': 1238,
                                    'date_adjusted': '2011-05-10T12:09:10Z',
                                    'geom': '0106000000010000000103000000010000000B000000DDEF0B89EEC665400CAB8C50D98E43C0AA7883AEBCC66540F6237BC40C8843C0D25EEE2300C7654002A1BF90B18543C0218DAFE279C76540391485E7938543C09EE81AACF7C76540E85798D99E8843C02E055F7296C765405BFD22B2598D43C0EA119EE595C765406BD26D895C8D43C087CDFB1423C76540723E2B1FB88E43C08DFCB0941BC7654054B82FB1C38E43C0A00948100AC76540FB04E1A5D38E43C0DDEF0B89EEC665400CAB8C50D98E43C0',
                                    'id': 1443053,
                                    'survey_reference': None,
                                },
                            },
                            {
                                '-': {
                                    'adjusted_nodes': 558,
                                    'date_adjusted': '2011-06-07T15:22:58Z',
                                    'geom': '01060000000100000001030000000100000018000000C43FCCA465D7654049FCE5EE4E6642C031DD1F0460D765406D606177F06542C064343C0760D765408E68DDEBED6542C0774AC25F66D7654003E4041CD46542C00442E6DF6AD765405B0AD914C76542C00F9E1F7B6BD76540B7354771C56542C099152AB96BD76540ED1D93E0C46542C03E5700F86CD76540F85610F9C16542C01E90DF366ED76540FDC68D11BF6542C056546E3273D765402D735F73B36542C056C5C5E175D76540EFB2BA30AD6542C06AC54D4277D76540182AC9FAA96542C09C400C8977D7654048F61C62A96542C03590D37C7AD76540168A743FA76542C0F38A07DA7CD7654069796568AA6542C0FF12A7497FD76540FD8AFFFBAF6542C0D5F5B5BE91D765406A7190D0F26542C049E06AF891D76540BCC23B6FF56542C08B3858D991D76540B6662B2FF96542C07E0C0C0F90D76540E2CF4B20006642C03FF664C98ED7654020CAD027046642C020E67C7C74D765406A7528F9476642C052A1D0E771D76540D9BFA1A64C6642C0C43FCCA465D7654049FCE5EE4E6642C0',
                                    'id': 1452332,
                                    'survey_reference': None,
                                }
                            },
                            {
                                '+': {
                                    'adjusted_nodes': 123,
                                    'date_adjusted': '2019-07-05T13:04:00+01:00',
                                    'geom': '01030000000100000005000000000000000000000000000000000000000000000000000000FCA9F1D24D62503FFCA9F1D24D62503FFCA9F1D24D62503FFCA9F1D24D62503F000000000000000000000000000000000000000000000000',
                                    'id': 9999999,
                                    'survey_reference': 'Null Island™ 🗺',
                                }
                            },
                        ],
                        'metaChanges': {},
                    }
                }
            }
        elif output_format == "html":
            _check_html_output(r.stdout)


@pytest.mark.parametrize("output_format", DIFF_OUTPUT_FORMATS)
@pytest.mark.parametrize(*V1_OR_V2)
def test_diff_table(
    structure_version, output_format, data_working_copy, geopackage, cli_runner
):
    """ diff the working copy against HEAD """
    data_archive = "table2" if structure_version == "2" else "table"
    with data_working_copy(data_archive) as (repo, wc):
        # empty
        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-", "--exit-code"]
        )
        assert r.exit_code == 0, r

        # make some changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()

            cur.execute(H.TABLE.INSERT, H.TABLE.RECORD)
            assert db.changes() == 1
            cur.execute(f'UPDATE {H.TABLE.LAYER} SET "OBJECTID"=9998 WHERE OBJECTID=1;')
            assert db.changes() == 1
            cur.execute(
                f"UPDATE {H.TABLE.LAYER} SET name='test', POP2000=9867 WHERE OBJECTID=2;"
            )
            assert db.changes() == 1
            cur.execute(f'DELETE FROM {H.TABLE.LAYER} WHERE "OBJECTID"=3;')
            assert db.changes() == 1

        r = cli_runner.invoke(
            ["diff", f"--output-format={output_format}", "--output=-"]
        )
        if output_format == "quiet":
            assert r.exit_code == 1, r
            assert r.stdout == ""
        elif output_format == "text":
            assert r.exit_code == 0, r
            assert r.stdout.splitlines() == [
                "--- countiestbl:OBJECTID=3",
                "-                                     AREA = 2529.9794",
                "-                                CNTY_FIPS = 065",
                "-                                     FIPS = 53065",
                "-                                     NAME = Stevens",
                "-                                  POP1990 = 30948.0",
                "-                                  POP2000 = 40652.0",
                "-                               POP90_SQMI = 12",
                "-                               STATE_FIPS = 53",
                "-                               STATE_NAME = Washington",
                "-                               Shape_Area = 0.7954858988987561",
                "-                               Shape_Leng = 4.876296245235406",
                "+++ countiestbl:OBJECTID=9999",
                "+                                     AREA = 1784.0634",
                "+                                CNTY_FIPS = 077",
                "+                                     FIPS = 27077",
                "+                                     NAME = Lake of the Gruffalo",
                "+                                  POP1990 = 4076.0",
                "+                                  POP2000 = 4651.0",
                "+                               POP90_SQMI = 2",
                "+                               STATE_FIPS = 27",
                "+                               STATE_NAME = Minnesota",
                "+                               Shape_Area = 0.565449933741451",
                "+                               Shape_Leng = 4.05545998243992",
                "--- countiestbl:OBJECTID=2",
                "+++ countiestbl:OBJECTID=2",
                "-                                     NAME = Ferry",
                "+                                     NAME = test",
                "-                                  POP2000 = 7199.0",
                "+                                  POP2000 = 9867.0",
                "--- countiestbl:OBJECTID=1",
                "+++ countiestbl:OBJECTID=9998",
            ]
        elif output_format == "geojson":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert len(odata["features"]) == 6
            assert odata == {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 3,
                            "AREA": 2529.9794,
                            "CNTY_FIPS": "065",
                            "FIPS": "53065",
                            "NAME": "Stevens",
                            "POP1990": 30948.0,
                            "POP2000": 40652.0,
                            "POP90_SQMI": 12,
                            "STATE_FIPS": "53",
                            "STATE_NAME": "Washington",
                            "Shape_Area": 0.795_485_898_898_756_1,
                            "Shape_Leng": 4.876_296_245_235_406,
                        },
                        "id": "D::3",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 9999,
                            "NAME": "Lake of the Gruffalo",
                            "STATE_NAME": "Minnesota",
                            "STATE_FIPS": "27",
                            "CNTY_FIPS": "077",
                            "FIPS": "27077",
                            "AREA": 1784.0634,
                            "POP1990": 4076.0,
                            "POP2000": 4651.0,
                            "POP90_SQMI": 2,
                            "Shape_Leng": 4.055_459_982_439_92,
                            "Shape_Area": 0.565_449_933_741_451,
                        },
                        "id": "I::9999",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 2,
                            "AREA": 2280.2319,
                            "CNTY_FIPS": "019",
                            "FIPS": "53019",
                            "NAME": "Ferry",
                            "POP1990": 6295.0,
                            "POP2000": 7199.0,
                            "POP90_SQMI": 3,
                            "STATE_FIPS": "53",
                            "STATE_NAME": "Washington",
                            "Shape_Area": 0.718_059_302_645_116_1,
                            "Shape_Leng": 3.786_160_993_863_997,
                        },
                        "id": "U-::2",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 2,
                            "NAME": "test",
                            "STATE_NAME": "Washington",
                            "STATE_FIPS": "53",
                            "CNTY_FIPS": "019",
                            "FIPS": "53019",
                            "AREA": 2280.2319,
                            "POP1990": 6295.0,
                            "POP2000": 9867.0,
                            "POP90_SQMI": 3,
                            "Shape_Leng": 3.786_160_993_863_997,
                            "Shape_Area": 0.718_059_302_645_116_1,
                        },
                        "id": "U+::2",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 1,
                            "AREA": 1784.0634,
                            "CNTY_FIPS": "077",
                            "FIPS": "27077",
                            "NAME": "Lake of the Woods",
                            "POP1990": 4076.0,
                            "POP2000": 4651.0,
                            "POP90_SQMI": 2,
                            "STATE_FIPS": "27",
                            "STATE_NAME": "Minnesota",
                            "Shape_Area": 0.565_449_933_741_450_9,
                            "Shape_Leng": 4.055_459_982_439_919,
                        },
                        "id": "U-::1",
                    },
                    {
                        "type": "Feature",
                        "geometry": None,
                        "properties": {
                            "OBJECTID": 9998,
                            "NAME": "Lake of the Woods",
                            "STATE_NAME": "Minnesota",
                            "STATE_FIPS": "27",
                            "CNTY_FIPS": "077",
                            "FIPS": "27077",
                            "AREA": 1784.0634,
                            "POP1990": 4076.0,
                            "POP2000": 4651.0,
                            "POP90_SQMI": 2,
                            "Shape_Leng": 4.055_459_982_439_919,
                            "Shape_Area": 0.565_449_933_741_450_9,
                        },
                        "id": "U+::9998",
                    },
                ],
            }
        elif output_format == "json":
            assert r.exit_code == 0, r
            odata = json.loads(r.stdout)
            assert (
                len(odata["sno.diff/v1+hexwkb"]["countiestbl"]["featureChanges"]) == 4
            )
            assert odata == {
                'sno.diff/v1+hexwkb': {
                    'countiestbl': {
                        'featureChanges': [
                            {
                                '+': {
                                    'AREA': 1784.0634,
                                    'CNTY_FIPS': '077',
                                    'FIPS': '27077',
                                    'NAME': 'Lake of the Woods',
                                    'OBJECTID': 9998,
                                    'POP1990': 4076.0,
                                    'POP2000': 4651.0,
                                    'POP90_SQMI': 2,
                                    'STATE_FIPS': '27',
                                    'STATE_NAME': 'Minnesota',
                                    'Shape_Area': 0.5654499337414509,
                                    'Shape_Leng': 4.055459982439919,
                                },
                                '-': {
                                    'AREA': 1784.0634,
                                    'CNTY_FIPS': '077',
                                    'FIPS': '27077',
                                    'NAME': 'Lake of the Woods',
                                    'OBJECTID': 1,
                                    'POP1990': 4076.0,
                                    'POP2000': 4651.0,
                                    'POP90_SQMI': 2,
                                    'STATE_FIPS': '27',
                                    'STATE_NAME': 'Minnesota',
                                    'Shape_Area': 0.5654499337414509,
                                    'Shape_Leng': 4.055459982439919,
                                },
                            },
                            {
                                '+': {
                                    'AREA': 2280.2319,
                                    'CNTY_FIPS': '019',
                                    'FIPS': '53019',
                                    'NAME': 'test',
                                    'OBJECTID': 2,
                                    'POP1990': 6295.0,
                                    'POP2000': 9867.0,
                                    'POP90_SQMI': 3,
                                    'STATE_FIPS': '53',
                                    'STATE_NAME': 'Washington',
                                    'Shape_Area': 0.7180593026451161,
                                    'Shape_Leng': 3.786160993863997,
                                },
                                '-': {
                                    'AREA': 2280.2319,
                                    'CNTY_FIPS': '019',
                                    'FIPS': '53019',
                                    'NAME': 'Ferry',
                                    'OBJECTID': 2,
                                    'POP1990': 6295.0,
                                    'POP2000': 7199.0,
                                    'POP90_SQMI': 3,
                                    'STATE_FIPS': '53',
                                    'STATE_NAME': 'Washington',
                                    'Shape_Area': 0.7180593026451161,
                                    'Shape_Leng': 3.786160993863997,
                                },
                            },
                            {
                                '-': {
                                    'AREA': 2529.9794,
                                    'CNTY_FIPS': '065',
                                    'FIPS': '53065',
                                    'NAME': 'Stevens',
                                    'OBJECTID': 3,
                                    'POP1990': 30948.0,
                                    'POP2000': 40652.0,
                                    'POP90_SQMI': 12,
                                    'STATE_FIPS': '53',
                                    'STATE_NAME': 'Washington',
                                    'Shape_Area': 0.7954858988987561,
                                    'Shape_Leng': 4.876296245235406,
                                }
                            },
                            {
                                '+': {
                                    'AREA': 1784.0634,
                                    'CNTY_FIPS': '077',
                                    'FIPS': '27077',
                                    'NAME': 'Lake of the Gruffalo',
                                    'OBJECTID': 9999,
                                    'POP1990': 4076.0,
                                    'POP2000': 4651.0,
                                    'POP90_SQMI': 2,
                                    'STATE_FIPS': '27',
                                    'STATE_NAME': 'Minnesota',
                                    'Shape_Area': 0.565449933741451,
                                    'Shape_Leng': 4.05545998243992,
                                }
                            },
                        ],
                        'metaChanges': {},
                    }
                }
            }
        elif output_format == "html":
            _check_html_output(r.stdout)


@pytest.mark.parametrize(
    "head_sha,head1_sha",
    [
        pytest.param(H.POINTS.HEAD_SHA, H.POINTS.HEAD1_SHA, id="commit_hash"),
        pytest.param(H.POINTS.HEAD_TREE_SHA, H.POINTS.HEAD1_TREE_SHA, id="tree_hash"),
    ],
)
def test_diff_rev_noop(head_sha, head1_sha, data_archive_readonly, cli_runner):
    """diff between trees / commits - no-op"""

    NOOP_SPECS = (
        f"{head_sha[:6]}...{head_sha[:6]}",
        f"{head_sha}...{head_sha}",
        f"{head1_sha}...{head1_sha}",
        "HEAD^1...HEAD^1",
        f"{head_sha}...",
        f"...{head_sha}",
    )

    with data_archive_readonly("points"):
        for spec in NOOP_SPECS:
            print(f"noop: {spec}")
            r = cli_runner.invoke(["diff", "--exit-code", spec])
            assert r.exit_code == 0, r


@pytest.mark.parametrize(
    "head_sha,head1_sha",
    [
        pytest.param(H.POINTS.HEAD_SHA, H.POINTS.HEAD1_SHA, id="commit_hash"),
        pytest.param(H.POINTS.HEAD_TREE_SHA, H.POINTS.HEAD1_TREE_SHA, id="tree_hash"),
    ],
)
def test_diff_rev_rev(head_sha, head1_sha, data_archive_readonly, cli_runner):
    """diff between trees / commits - no-op"""

    F_SPECS = (
        f"{head1_sha}...{head_sha}",
        f"{head1_sha}...",
        "HEAD^1...HEAD",
    )

    R_SPECS = (
        f"{head_sha}...{head1_sha}",
        f"...{head1_sha}",
        "HEAD...HEAD^1",
    )

    CHANGE_IDS = {
        (1182, 1182),
        (1181, 1181),
        (1168, 1168),
        (1166, 1166),
        (1095, 1095),
    }

    with data_archive_readonly("points"):
        for spec in F_SPECS:
            print(f"fwd: {spec}")
            r = cli_runner.invoke(["diff", "--exit-code", "-o", "json", spec])
            assert r.exit_code == 1, r
            odata = json.loads(r.stdout)["sno.diff/v1+hexwkb"]
            assert len(odata[H.POINTS.LAYER]["featureChanges"]) == 5
            assert len(odata[H.POINTS.LAYER]["metaChanges"]) == 0

            change_ids = {
                (
                    f.get('-', {}).get(H.POINTS.LAYER_PK),
                    f.get('+', {}).get(H.POINTS.LAYER_PK),
                )
                for f in odata[H.POINTS.LAYER]["featureChanges"]
            }
            assert change_ids == CHANGE_IDS
            # this commit _adds_ names
            change_names = {
                (f['-']["name"], f['+']["name"])
                for f in odata[H.POINTS.LAYER]["featureChanges"]
            }
            assert not any(n[0] for n in change_names)
            assert all(n[1] for n in change_names)

        for spec in R_SPECS:
            print(f"rev: {spec}")
            r = cli_runner.invoke(["diff", "--exit-code", "-o", "json", spec])
            assert r.exit_code == 1, r
            odata = json.loads(r.stdout)["sno.diff/v1+hexwkb"]
            assert len(odata[H.POINTS.LAYER]["featureChanges"]) == 5
            assert len(odata[H.POINTS.LAYER]["metaChanges"]) == 0
            change_ids = {
                (
                    f.get('-', {}).get(H.POINTS.LAYER_PK),
                    f.get('+', {}).get(H.POINTS.LAYER_PK),
                )
                for f in odata[H.POINTS.LAYER]["featureChanges"]
            }
            assert change_ids == CHANGE_IDS
            # so names are _removed_
            change_names = {
                (f['-']["name"], f['+']["name"])
                for f in odata[H.POINTS.LAYER]["featureChanges"]
            }
            assert all(n[0] for n in change_names)
            assert not any(n[1] for n in change_names)


def test_diff_rev_wc(data_working_copy, geopackage, cli_runner):
    """ diff the working copy against commits """
    # ID  R0  ->  R1  ->  WC
    # 1   a       a1      a
    # 2   b       b1      b1
    # 3   c       c       c1
    # 4   d       d1      d2
    # 5   e       e1      e*
    # 6   f       f*      f+
    # 7   g       g*      -
    # 8   -       h+      h1
    # 9   -       i+      i*
    # 10  -       j+      j
    # 11  -       -       k+
    # 12  l       l*      l1+

    # Legend:
    #     x     existing
    #     xN    edit
    #     x*    delete
    #     x+    insert
    #     -     not there

    # Columns: id,value

    R0 = "c4ee0b7c540492bcaff2b27aa5c22a4b08e47d13"
    R1 = "020da410459f08b69cbad4233c40a6b05706bda0"  # HEAD

    with data_working_copy("editing") as (repo, wc):
        # empty HEAD -> no working copy changes
        # r = cli_runner.invoke(["diff", "--exit-code", f"HEAD"])
        # assert r.exit_code == 0, r

        # make the R1 -> WC changes
        db = geopackage(wc)
        with db:
            cur = db.cursor()

            EDITS = ((1, "a"), (3, "c1"), (4, "d2"), (8, "h1"))
            for pk, val in EDITS:
                cur.execute("UPDATE editing SET value = ? WHERE id = ?;", (val, pk))
                assert db.changes() == 1

            cur.execute("DELETE FROM editing WHERE id IN (5, 9);")
            assert db.changes() == 2

            cur.execute(
                "INSERT INTO editing (id, value) VALUES (6, 'f'), (11, 'k'), (12, 'l1');"
            )
            assert db.changes() == 3

        def _extract(diff_json):
            ds = {}
            for f in odata["editing"]["featureChanges"]:
                old = f.get('-')
                new = f.get('+')
                pk = old["id"] if old else new["id"]
                v_old = old["value"] if old else None
                v_new = new["value"] if new else None
                ds[pk] = (v_old, v_new)
            return ds

        # changes from HEAD (R1 -> WC)
        r = cli_runner.invoke(["diff", "--exit-code", "-o", "json", R1])
        assert r.exit_code == 1, r
        odata = json.loads(r.stdout)["sno.diff/v1+hexwkb"]
        ddata = _extract(odata)
        assert ddata == {
            1: ('a1', 'a'),
            3: ('c', 'c1'),
            4: ('d1', 'd2'),
            5: ('e1', None),
            6: (None, 'f'),
            8: ('h', 'h1'),
            9: ('i', None),
            11: (None, 'k'),
            12: (None, 'l1'),
        }

        # changes from HEAD^1 (R0 -> WC)
        r = cli_runner.invoke(["diff", "--exit-code", "-o", "json", R0])
        assert r.exit_code == 1, r
        odata = json.loads(r.stdout)["sno.diff/v1+hexwkb"]
        ddata = _extract(odata)
        assert ddata == {
            2: ("b", "b1"),
            3: ("c", "c1"),
            4: ("d", "d2"),
            5: ("e", None),
            7: ("g", None),
            8: (None, "h1"),
            10: (None, "j"),
            11: (None, "k"),
            12: ("l", "l1"),
        }


def test_diff_object_union():
    FakeDataset = collections.namedtuple("Dataset", ["path"])

    ds1 = FakeDataset("ds1")
    ds2 = FakeDataset("ds2")

    # Diff(self, dataset_or_diff, meta=None, inserts=None, updates=None, deletes=None)
    diff1 = Diff(ds1)
    diff2 = Diff(ds2)

    assert len(diff1) == 0
    assert len(diff2) == 0

    diff3 = diff1 | diff2
    assert diff3 is not diff1
    assert diff3 is not diff2
    assert set(diff3.datasets()) == {ds1, ds2}

    diff1 |= diff2
    assert set(diff1.datasets()) == {ds1, ds2}

    diff4 = Diff(ds1)
    with pytest.raises(ValueError):
        diff4 | diff1

    with pytest.raises(ValueError):
        diff4 |= diff1


FakeDataset = collections.namedtuple("Dataset", ["path", "primary_key"])


def test_diff_object_add():

    ds1 = FakeDataset("ds1", "pk")
    ds2 = FakeDataset("ds2", "pk")

    NULL_DIFF = {"D": {}, "I": [], "META": {}, "U": {}}

    # Diff(self, dataset_or_diff, meta=None, inserts=None, updates=None, deletes=None)
    diff1 = Diff(ds1)
    diff2 = Diff(ds2)

    assert len(diff1) == 0
    assert len(diff2) == 0

    diff3 = diff1 + diff2
    assert diff3 is not diff1
    assert diff3 is not diff2
    assert set(diff3.datasets()) == {ds1, ds2}

    diff1 += diff2
    assert set(diff1.datasets()) == {ds1, ds2}

    diff4 = Diff(
        ds1,
        inserts=[{"pk": 20}],
        updates={"10": ({"pk": 10}, {"pk": 11})},
        deletes={"30": {"pk": 30}},
    )
    diff5 = diff4 + diff1
    assert diff5[ds1] == {
        "META": {},
        "I": [{"pk": 20}],
        "U": {"10": ({"pk": 10}, {"pk": 11})},
        "D": {"30": {"pk": 30}},
    }
    assert diff5[ds2] == NULL_DIFF

    diff4 += diff1
    assert diff4[ds1] == {
        "META": {},
        "I": [{"pk": 20}],
        "U": {"10": ({"pk": 10}, {"pk": 11})},
        "D": {"30": {"pk": 30}},
    }
    assert diff4[ds2] == NULL_DIFF


# ID  R0  ->  R1  ->  R2
# 1   a       a1      a
# 2   b       b1      b1
# 3   c       c       c1
# 4   d       d1      d2
# 5   e       e1      e*
# 6   f       f*      f+
# 7   g       g*      -
# 8   -       h+      h1
# 9   -       i+      i*
# 10  -       j+      j
# 11  -       -       k+
# 12  l       l*      l1+
DIFF_R1 = {
    "I": [{"pk": 8, "v": "h"}, {"pk": 9, "v": "i"}, {"pk": 10, "v": "j"}],
    "U": {
        "1": ({"pk": 1, "v": "a"}, {"pk": 1, "v": "a1"}),
        "2": ({"pk": 2, "v": "b"}, {"pk": 2, "v": "b1"}),
        "4": ({"pk": 4, "v": "d"}, {"pk": 4, "v": "d1"}),
        "5": ({"pk": 5, "v": "e"}, {"pk": 5, "v": "e1"}),
    },
    "D": {
        "6": {"pk": 6, "v": "f"},
        "7": {"pk": 7, "v": "g"},
        "12": {"pk": 12, "v": "l"},
    },
}
DIFF_R2 = {
    "I": [{"pk": 6, "v": "f"}, {"pk": 11, "v": "k"}, {"pk": 12, "v": "l1"}],
    "U": {
        "1": ({"pk": 1, "v": "a1"}, {"pk": 1, "v": "a"}),
        "3": ({"pk": 3, "v": "c"}, {"pk": 3, "v": "c1"}),
        "4": ({"pk": 4, "v": "d1"}, {"pk": 4, "v": "d2"}),
        "8": ({"pk": 8, "v": "h"}, {"pk": 8, "v": "h1"}),
    },
    "D": {"5": {"pk": 5, "v": "e1"}, "9": {"pk": 9, "v": "i"}},
}
DIFF_R0_R2 = {
    "I": [{"pk": 8, "v": "h1"}, {"pk": 10, "v": "j"}, {"pk": 11, "v": "k"}],
    "U": {
        "2": ({"pk": 2, "v": "b"}, {"pk": 2, "v": "b1"}),
        "3": ({"pk": 3, "v": "c"}, {"pk": 3, "v": "c1"}),
        "4": ({"pk": 4, "v": "d"}, {"pk": 4, "v": "d2"}),
        "12": ({"pk": 12, "v": "l"}, {"pk": 12, "v": "l1"}),
    },
    "D": {"5": {"pk": 5, "v": "e"}, "7": {"pk": 7, "v": "g"}},
    "META": {},
}


def test_diff_object_add_2():
    ds = FakeDataset("ds", "pk")

    diff1 = Diff(ds, inserts=DIFF_R1["I"], updates=DIFF_R1["U"], deletes=DIFF_R1["D"])
    diff2 = Diff(ds, inserts=DIFF_R2["I"], updates=DIFF_R2["U"], deletes=DIFF_R2["D"])

    diff3 = diff1 + diff2
    assert DIFF_R0_R2["I"] == diff3[ds]["I"]
    assert DIFF_R0_R2["U"] == diff3[ds]["U"]
    assert DIFF_R0_R2["D"] == diff3[ds]["D"]

    diff1 += diff2
    assert DIFF_R0_R2["I"] == diff1[ds]["I"]
    assert DIFF_R0_R2["U"] == diff1[ds]["U"]
    assert DIFF_R0_R2["D"] == diff1[ds]["D"]

    assert diff3 == diff1


def test_diff_object_eq_reverse():
    ds = FakeDataset("ds", "pk")

    diff1 = Diff(ds, inserts=DIFF_R1["I"], updates=DIFF_R1["U"], deletes=DIFF_R1["D"])
    diff2 = Diff(ds, inserts=DIFF_R2["I"], updates=DIFF_R2["U"], deletes=DIFF_R2["D"])

    diff3 = diff1 + diff2

    diff4 = ~diff3
    assert diff4 != diff3
    assert diff4 == ~diff3
    assert len(diff4[ds]["I"]) == len(diff3[ds]["D"])
    assert len(diff4[ds]["D"]) == len(diff3[ds]["I"])
    assert len(diff4[ds]["U"]) == len(diff3[ds]["U"])
    assert list(diff4[ds]["U"].values()) == [
        (v1, v0) for v0, v1 in diff3[ds]["U"].values()
    ]


def test_diff_object_add_reverse():
    """
    Check that ~(A + B) == (~B + ~A)
    """
    ds = FakeDataset("ds", "pk")

    diff1 = Diff(ds, inserts=DIFF_R1["I"], updates=DIFF_R1["U"], deletes=DIFF_R1["D"])
    diff2 = Diff(ds, inserts=DIFF_R2["I"], updates=DIFF_R2["U"], deletes=DIFF_R2["D"])

    diff3 = diff1 + diff2

    r2 = ~diff2
    r1 = ~diff1
    r2r1 = r2 + r1
    i3 = ~diff3
    assert i3 == r2r1


def test_diff_3way(data_working_copy, geopackage, cli_runner, insert, request):
    with data_working_copy("points") as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        # new branch
        r = cli_runner.invoke(["checkout", "-b", "changes"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/changes"

        # make some changes
        db = geopackage(wc)
        insert(db)
        insert(db)
        b_commit_id = insert(db)
        assert repo.head.target.hex == b_commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != b_commit_id
        m_commit_id = insert(db)
        H.git_graph(request, "pre-merge-master")

        # Three dots diff should show both sets of changes.
        r = cli_runner.invoke(["diff", "-o", "json", f"{m_commit_id}...{b_commit_id}"])
        assert r.exit_code == 0, r
        featureChanges = json.loads(r.stdout)["sno.diff/v1+hexwkb"][
            "nz_pa_points_topo_150k"
        ]["featureChanges"]
        assert len(featureChanges) == 4

        r = cli_runner.invoke(["diff", "-o", "json", f"{b_commit_id}...{m_commit_id}"])
        assert r.exit_code == 0, r
        featureChanges = json.loads(r.stdout)["sno.diff/v1+hexwkb"][
            "nz_pa_points_topo_150k"
        ]["featureChanges"]
        assert len(featureChanges) == 4

        # Two dots diff should show only one set of changes - the changes on the target branch.
        r = cli_runner.invoke(["diff", "-o", "json", f"{m_commit_id}..{b_commit_id}"])
        assert r.exit_code == 0, r
        featureChanges = json.loads(r.stdout)["sno.diff/v1+hexwkb"][
            "nz_pa_points_topo_150k"
        ]["featureChanges"]
        assert len(featureChanges) == 3

        r = cli_runner.invoke(["diff", "-o", "json", f"{b_commit_id}..{m_commit_id}"])
        assert r.exit_code == 0, r
        featureChanges = json.loads(r.stdout)["sno.diff/v1+hexwkb"][
            "nz_pa_points_topo_150k"
        ]["featureChanges"]
        assert len(featureChanges) == 1


@pytest.mark.parametrize("output_format", SHOW_OUTPUT_FORMATS)
@pytest.mark.parametrize(*V1_OR_V2)
def test_show_points_HEAD(
    structure_version, output_format, data_archive_readonly, cli_runner
):
    """
    Show a patch; ref defaults to HEAD
    """
    data_archive = "points2" if structure_version == "2" else "points"
    with data_archive_readonly(data_archive):
        r = cli_runner.invoke(["show", f"--output-format={output_format}", "HEAD"])
        assert r.exit_code == 0, r

        if output_format == 'text':
            commit_hash = r.stdout[7:47]
            # TODO - maybe make the diff order consistent, so we don't need to sort here?
            blocks = re.split("(?=---)", r.stdout)
            assert blocks[0].splitlines() == [
                f'commit {commit_hash}',
                'Author: Robert Coup <robert@coup.net.nz>',
                'Date:   Thu Jun 20 15:28:33 2019 +0100',
                '',
                '    Improve naming on Coromandel East coast',
                '',
            ]

            assert [b.splitlines() for b in sorted(blocks[1:])] == [
                [
                    '--- nz_pa_points_topo_150k:fid=1095',
                    '+++ nz_pa_points_topo_150k:fid=1095',
                    '-                               macronated = N',
                    '+                               macronated = Y',
                    '-                                     name = ␀',
                    '+                                     name = Harataunga (Rākairoa)',
                    '',
                    '-                               name_ascii = ␀',
                    '+                               name_ascii = Harataunga (Rakairoa)',
                    '',
                ],
                [
                    '--- nz_pa_points_topo_150k:fid=1166',
                    '+++ nz_pa_points_topo_150k:fid=1166',
                    '-                                     name = ␀',
                    '+                                     name = Oturu',
                    '-                               name_ascii = ␀',
                    '+                               name_ascii = Oturu',
                ],
                [
                    '--- nz_pa_points_topo_150k:fid=1168',
                    '+++ nz_pa_points_topo_150k:fid=1168',
                    '-                                     name = ␀',
                    '+                                     name = Tairua',
                    '-                               name_ascii = ␀',
                    '+                               name_ascii = Tairua',
                ],
                [
                    '--- nz_pa_points_topo_150k:fid=1181',
                    '+++ nz_pa_points_topo_150k:fid=1181',
                    '-                               macronated = N',
                    '+                               macronated = Y',
                    '-                                     name = ␀',
                    '+                                     name = Ko Te Rā Matiti (Wharekaho)',
                    '-                               name_ascii = ␀',
                    '+                               name_ascii = Ko Te Ra Matiti (Wharekaho)',
                ],
                [
                    '--- nz_pa_points_topo_150k:fid=1182',
                    '+++ nz_pa_points_topo_150k:fid=1182',
                    '-                               macronated = N',
                    '+                               macronated = Y',
                    '-                                     name = ␀',
                    '+                                     name = Ko Te Rā Matiti (Wharekaho)',
                    '-                               name_ascii = ␀',
                    '+                               name_ascii = Ko Te Ra Matiti (Wharekaho)',
                ],
            ]
        elif output_format == 'json':
            j = json.loads(r.stdout)
            # check the diff's present, but this test doesn't need to have hundreds of lines
            # to know exactly what it is (we have diff tests above)
            assert 'sno.diff/v1+hexwkb' in j
            assert j['sno.patch/v1'] == {
                'authorEmail': 'robert@coup.net.nz',
                'authorName': 'Robert Coup',
                'authorTime': '2019-06-20T14:28:33Z',
                'authorTimeOffset': '+01:00',
                'message': 'Improve naming on Coromandel East coast',
            }


@pytest.mark.parametrize("output_format", SHOW_OUTPUT_FORMATS)
@pytest.mark.parametrize(*V1_OR_V2)
def test_show_polygons_initial(
    structure_version, output_format, data_archive_readonly, cli_runner
):
    data_archive = "polygons2" if structure_version == "2" else "polygons"

    """Make sure we can show the initial commit"""
    with data_archive_readonly("polygons"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r
        initial_commit = re.findall("commit ([0-9a-f]+)\n", r.stdout)[-1]

        r = cli_runner.invoke(
            ["show", f"--output-format={output_format}", initial_commit]
        )
        assert r.exit_code == 0, r

        if output_format == 'text':
            assert r.stdout.splitlines()[0:11] == [
                'commit 1fb58eb54237c6e7bfcbd7ea65dc999a164b78ec',
                'Author: Robert Coup <robert@coup.net.nz>',
                'Date:   Mon Jul 22 12:05:39 2019 +0100',
                '',
                '    Import from nz-waca-adjustments.gpkg',
                '',
                '+++ nz_waca_adjustments:id=1424927',
                '+                           adjusted_nodes = 1122',
                '+                            date_adjusted = 2011-03-25T07:30:45Z',
                '+                                     geom = MULTIPOLYGON(...)',
                '+                         survey_reference = ␀',
            ]
        elif output_format == 'json':
            j = json.loads(r.stdout)
            assert 'sno.diff/v1+hexwkb' in j
            assert j["sno.patch/v1"] == {
                "authorEmail": "robert@coup.net.nz",
                "authorName": "Robert Coup",
                "authorTime": "2019-07-22T11:05:39Z",
                "authorTimeOffset": "+01:00",
                "message": "Import from nz-waca-adjustments.gpkg",
            }


def test_show_json_format(data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["show", f"-o", "json", "--json-style=compact", "HEAD"])

        assert r.exit_code == 0, r
        # output is compact, no indentation
        assert '"sno.diff/v1+hexwkb": {"' in r.stdout
