use teo_runtime::sort::Sort;

pub trait SortExt {

    fn from_desc_bool(desc: bool) -> Sort;

    fn from_mysql_str(s: &str) -> Option<Sort>;

    fn from_str(s: &str) -> Option<Sort>;

    fn to_str(&self) -> &'static str;
}

impl SortExt for Sort {

    fn from_desc_bool(desc: bool) -> Sort {
        match desc {
            true => Sort::Desc,
            false => Sort::Asc,
        }
    }

    fn from_mysql_str(s: &str) -> Option<Sort> {
        match s {
            "A" => Some(Sort::Asc),
            "D" => Some(Sort::Desc),
            _ => None,
        }
    }

    fn from_str(s: &str) -> Option<Sort> {
        match s {
            "ASC" => Some(Sort::Asc),
            "DESC" => Some(Sort::Desc),
            _ => None,
        }
    }

    fn to_str(&self) -> &'static str {
        match self {
            Sort::Asc => "ASC",
            Sort::Desc => "DESC",
        }
    }
}