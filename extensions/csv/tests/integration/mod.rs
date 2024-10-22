use rstest::rstest;
use scr_core::{
    extension::{Extension, ExtensionRegistry},
    operators::{
        count::create_op_count,
        foreach::create_op_foreach,
        format::create_op_format,
        regex::{create_op_regex, create_op_regex_with_opts, RegexOptions},
        select::create_op_select,
        utils::readable::MutexedReadableTargetOwner,
    },
    options::context_builder::ContextBuilder,
    scr_error::ScrError,
    utils::test_utils::SliceReader,
};
use scr_ext_csv::{csv::create_op_csv, CsvExtension};
use scr_ext_utils::{head::create_op_head, sum::create_op_sum};
use std::{
    fmt::Write,
    io::Cursor,
    sync::{Arc, LazyLock},
};

pub static CSV_EXTENSION_REGISTRY: LazyLock<Arc<ExtensionRegistry>> =
    LazyLock::new(|| {
        ExtensionRegistry::new([Box::<dyn Extension>::from(Box::new(
            CsvExtension::default(),
        ))])
    });

#[test]
fn first_column_becomes_output() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "a,b,c\n1,2\r\nx,y,z,w".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["a", "1", "x"]);
    Ok(())
}

#[test]
fn end_correctly_truncates_first_column() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "a,b,c\nb\nxyz".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .run_collect_stringified()?;
    assert_eq!(res, ["a", "b", "xyz"]);
    Ok(())
}

#[test]
fn access_second_field() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "a,b,c\r\nb\nx,y,".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_format("{_1:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["\"b\"", "null", "\"y\""]);
    Ok(())
}

#[test]
fn last_row_filled_up_with_nulls() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "1,\r\na,b\nx".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_format("{_1:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["\"\"", "\"b\"", "null"]);
    Ok(())
}

#[test]
fn csv_parses_integers() -> Result<(), ScrError> {
    let target = MutexedReadableTargetOwner::new(SliceReader::new(
        "1,2,3\r\na,b,c\nx".as_bytes(),
    ));
    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_format("{_1:?}").unwrap())
        .run_collect_stringified()?;
    assert_eq!(res, ["2", "\"b\"", "null"]);
    Ok(())
}

#[rstest]
#[case(5, 3)]
#[case(1024, 4096)]
fn multibatch(
    #[case] batch_size: usize,
    #[case] count: usize,
) -> Result<(), ScrError> {
    let mut input = String::new();

    for i in 0..count {
        input.write_fmt(format_args!("{i},\n")).unwrap();
    }

    let target = MutexedReadableTargetOwner::new(Cursor::new(input));

    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_sum())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [(count * (count - 1) / 2) as i64]);
    Ok(())
}

#[test]
fn head() -> Result<(), ScrError> {
    let mut input = String::new();

    for i in 0..10 {
        input.write_fmt(format_args!("{i},\n")).unwrap();
    }

    let target = MutexedReadableTargetOwner::new(Cursor::new(input));

    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .set_batch_size(3)
        .unwrap()
        .add_op(create_op_csv(target.create_target(), false, false))
        .add_op(create_op_head(5))
        .run_collect_stringified()?;
    assert_eq!(res, ["0", "1", "2", "3", "4"]);
    Ok(())
}

// Information courtesy of IMDb (https://www.imdb.com). Used with permission.
const IMDB_CSV_EXAMPLE: &str = r#"nconst,primaryName,birthYear,deathYear,primaryProfession,knownForTitles
nm0000001,Fred Astaire,1899,1987,actor;miscellaneous;producer,tt0072308;tt0050419;tt0053137;tt0027125
nm0000002,Lauren Bacall,1924,2014,actress;soundtrack;archive_footage,tt0037382;tt0075213;tt0117057;tt0038355
nm0000003,Brigitte Bardot,1934,\N,actress;music_department;producer,tt0057345;tt0049189;tt0056404;tt0054452
nm0000004,John Belushi,1949,1982,actor;writer;music_department,tt0072562;tt0077975;tt0080455;tt0078723
nm0000005,Ingmar Bergman,1918,2007,writer;director;actor,tt0050986;tt0083922;tt0050976;tt0069467
nm0000006,Ingrid Bergman,1915,1982,actress;producer;soundtrack,tt0034583;tt0036855;tt0038109;tt0038787
nm0000007,Humphrey Bogart,1899,1957,actor;producer;miscellaneous,tt0034583;tt0037382;tt0042593;tt0043265
nm0000008,Marlon Brando,1924,2004,actor;director;writer,tt0078788;tt0068646;tt0047296;tt0070849
nm0000009,Richard Burton,1925,1984,actor;producer;director,tt0061184;tt0087803;tt0059749;tt0057877
nm0000010,James Cagney,1899,1986,actor;director;producer,tt0029870;tt0031867;tt0042041;tt0055256
nm0000011,Gary Cooper,1901,1961,actor;stunts;producer,tt0044706;tt0034167;tt0035896;tt0027996
nm0000012,Bette Davis,1908,1989,actress;make_up_department;producer,tt0042192;tt0056687;tt0031210;tt0035140
nm0000013,Doris Day,1922,2019,actress;producer;miscellaneous,tt0048317;tt0045591;tt0053172;tt0049470
nm0000014,Olivia de Havilland,1916,2020,actress;soundtrack;archive_footage,tt0031381;tt0041452;tt0029843;tt0040806
nm0000015,James Dean,1931,1955,actor;miscellaneous;archive_footage,tt0048028;tt0048545;tt0049261;tt0040051
nm0000016,Georges Delerue,1925,1992,composer;music_department;actor,tt0091763;tt0096320;tt0069946;tt0080610
nm0000017,Marlene Dietrich,1901,1992,actress;music_department;soundtrack,tt0051201;tt0055031;tt0052311;tt0021156
nm0000018,Kirk Douglas,1916,2020,actor;producer;director,tt0080736;tt0054331;tt0049456;tt0050825
nm0000019,Federico Fellini,1920,1993,writer;director;actor,tt0056801;tt0050783;tt0047528;tt0053779
"#;

#[rstest]
#[case(7)]
#[case(2)]
fn imdb_actor_count(#[case] batch_size: usize) -> Result<(), ScrError> {
    let target =
        MutexedReadableTargetOwner::new(Cursor::new(IMDB_CSV_EXAMPLE));

    let res = ContextBuilder::with_exts(CSV_EXTENSION_REGISTRY.clone())
        .set_batch_size(batch_size)
        .unwrap()
        .add_op(create_op_csv(target.create_target(), true, false))
        .add_op(create_op_select("_4"))
        .add_op(create_op_foreach([
            create_op_regex_with_opts(
                "(?<>.*?)(;|$)",
                RegexOptions {
                    multimatch: true,
                    ..Default::default()
                },
            )?,
            create_op_regex("actor|actress")?,
            create_op_head(1),
        ]))
        .add_op(create_op_count())
        .run_collect_as::<i64>()?;
    assert_eq!(res, [19]);
    Ok(())
}
