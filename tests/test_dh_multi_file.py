import polars as pl

from src.services.dh_data_service import read_data_dh


def _write_csv(path, rows, header):
    path.write_text("\n".join([header] + rows))


def test_read_multiple_and_concat(tmp_path):
    header = "NUMBER,STATUS,WORK CENTER TYPE,DEFECT TYPES,DEFECT COUNTERMEASURES,PRIORITY,DESCRIPTION,FOUND DURING,REPORTED AT"

    rows1 = [
        '1,OPEN,LINE_A,,cm1,HIGH,"desc a",CIL,2025-11-01 08:00:00',
        '2,CLOSED,LINE_B,,cm2,LOW,"desc b",OTHER,2025-11-02 09:00:00',
    ]

    rows2 = [
        '3,OPEN,LINE_A,,cm3,MEDIUM,"desc c",CIL,2025-11-03 10:00:00',
    ]

    f1 = tmp_path / "a.csv"
    f2 = tmp_path / "b.csv"
    _write_csv(f1, rows1, header)
    _write_csv(f2, rows2, header)

    # read each file independently using the service
    df1 = read_data_dh(str(f1))
    df2 = read_data_dh(str(f2))

    assert isinstance(df1, pl.DataFrame)
    assert isinstance(df2, pl.DataFrame)

    # concatenate like the UI does
    df_all = pl.concat([df1, df2], how="vertical")

    assert df_all.height == df1.height + df2.height
    # Basic column expectations
    for col in [
        "NUMBER",
        "STATUS",
        "WORK CENTER TYPE",
        "PRIORITY",
        "DESCRIPTION",
        "REPORTED AT",
    ]:
        assert col in df_all.columns
