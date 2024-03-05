use std::borrow::Cow;
use std::collections::BTreeMap;
use indexmap::indexmap;
use itertools::Itertools;
use maplit::{btreemap};
use once_cell::sync::Lazy;
use teo_parser::r#type::Type;
use teo_result::{Error, Result};
use crate::schema::dialect::SQLDialect;
use crate::schema::value::encode::{IfIMode, SQLEscape, ToLike, ToSQLString, ToWrapped, ValueToSQLString, WrapInArray};
use crate::stmts::select::r#where::{ToWrappedSQLString, WhereClause};
use crate::stmts::select::r#where::WhereClause::{And, Not};
use crate::stmts::SQL;
use teo_runtime::model::{Model, object::Object, object::input::Input};
use teo_runtime::model::field::column_named::ColumnNamed;
use teo_runtime::model::field::typed::Typed;
use teo_runtime::namespace::Namespace;
use teo_teon::Value;

pub(crate) struct Query { }

impl Query {

    pub(crate) fn where_item(lhs: impl AsRef<str>, op: &str, rhs: &str) -> String {
        format!("{} {} {}", lhs.as_ref(), op, rhs)
    }

    pub(crate) fn where_from_identifier(object: &Object, dialect: SQLDialect) -> String {
        Self::where_from_value(object.model(), &object.identifier(), dialect)
    }

    pub(crate) fn where_from_previous_identifier(object: &Object, dialect: SQLDialect) -> String {
        Self::where_from_value(object.model(), &object.previous_identifier(), dialect)
    }

    fn where_entry_array(
        column_name: &str,
        r#type: &Type,
        optional: bool,
        value: &Value,
        op: &str,
        dialect: SQLDialect,
    ) -> String {
        let arr_val = value.as_array().unwrap();
        let mut arr: Vec<String> = Vec::new();
        for val in arr_val {
            arr.push(val.to_sql_string(r#type, optional, dialect));
        }
        Query::where_item(column_name, op, &arr.join(", ").to_wrapped())
    }

    fn where_entry_item(
        column_name: &str,
        r#type: &Type,
        optional: bool,
        value: &Value,
        dialect: SQLDialect,
    ) -> String {
        let column_name = escape_wisdom(column_name, dialect);
        if let Some(map) = value.as_dictionary() {
            let mut result: Vec<String> = vec![];
            for (key, value) in map {
                match key.as_str() {
                    "equals" => if value.is_null() {
                        result.push(Self::where_item(&column_name, "IS", "NULL"));
                    } else {
                        result.push(Self::where_item(&column_name, "=", &value.to_sql_string(r#type, optional, dialect)));
                    }
                    "not" => if value.is_null() {
                        result.push(Self::where_item(&column_name, "IS NOT", "NULL"));
                    } else {
                        result.push(Self::where_item(&column_name, "<>", &value.to_sql_string(r#type, optional, dialect)));
                    }
                    "gt" => {
                        result.push(Self::where_item(&column_name, ">", &value.to_sql_string(r#type, false, dialect)));
                    }
                    "gte" => {
                        result.push(Self::where_item(&column_name, ">=", &value.to_sql_string(r#type, false, dialect)));
                    }
                    "lt" => {
                        result.push(Self::where_item(&column_name, "<", &value.to_sql_string(r#type, false, dialect)));
                    }
                    "lte" => {
                        result.push(Self::where_item(&column_name, "<=", &value.to_sql_string(r#type, false, dialect)));
                    }
                    "in" => {
                        if !value.as_array().unwrap().is_empty() {
                            result.push(Self::where_entry_array(&column_name, r#type, optional, value, "IN", dialect));
                        } else {
                            result.push("FALSE".to_owned())
                        }
                    }
                    "notIn" => {
                        if !value.as_array().unwrap().is_empty() {
                            result.push(Self::where_entry_array(&column_name, r#type, optional, value, "NOT IN", dialect));
                        } else {
                            result.push("TRUE".to_owned())
                        }
                    }
                    "contains" => {
                        let i_mode = Input::has_i_mode(map);
                        result.push(Self::where_item(&column_name.to_i_mode(i_mode), "LIKE", &value.to_sql_string(r#type, false, dialect).to_like(true, true).to_i_mode(i_mode)));
                    }
                    "startsWith" => {
                        let i_mode = Input::has_i_mode(map);
                        result.push(Self::where_item(&column_name.to_i_mode(i_mode), "LIKE", &value.to_sql_string(r#type, false, dialect).to_like(false, true).to_i_mode(i_mode)));
                    }
                    "endsWith" => {
                        let i_mode = Input::has_i_mode(map);
                        result.push(Self::where_item(&column_name.to_i_mode(i_mode), "LIKE", &value.to_sql_string(r#type, false, dialect).to_like(true, false).to_i_mode(i_mode)));
                    }
                    "matches" => {
                        let i_mode = Input::has_i_mode(map);
                        result.push(Self::where_item(&column_name.to_i_mode(i_mode), "REGEXP", &value.to_sql_string(r#type, false, dialect).to_i_mode(i_mode)));
                    }
                    "mode" => { }
                    "has" => {
                        let element_type = r#type.as_array().unwrap();
                        result.push(Self::where_item(&column_name, "@>", &value.to_sql_string_array_arg(element_type.unwrap_optional(), element_type.is_optional(), dialect).wrap_in_array()));
                    }
                    "hasEvery" => {
                        result.push(Self::where_item(&column_name, "@>", &value.to_sql_string_array_arg(r#type, false, dialect)));
                    }
                    "hasSome" => {
                        result.push(Self::where_item(&column_name, "&&", &value.to_sql_string_array_arg(r#type, false, dialect)));
                    }
                    "isEmpty" => {
                        result.push(Self::where_item(&format!("ARRAY_LENGTH({})", &column_name), "=", "0"));
                    }
                    "length" => {
                        result.push(Self::where_item(&format!("ARRAY_LENGTH({})", &column_name), "=", &value.to_sql_string(&Type::Int64, false, dialect)));
                    }
                    "_count" => {
                        result.push(Self::where_entry_item(&format!("COUNT({})", &column_name), &Type::Int64, false, value, dialect));
                    }
                    "_avg" | "_sum" => {
                        result.push(Self::where_entry_item(&format!("{}({})", key[1..].to_uppercase(), &column_name), &Type::Float, true, value, dialect));
                    }
                    "_min" | "_max" => {
                        result.push(Self::where_entry_item(&format!("{}({})", key[1..].to_uppercase(), &column_name), r#type, optional, value, dialect));
                    }
                    _ => panic!("Unhandled key."),
                }
            }
            And(result).to_wrapped_string(dialect)
        } else {
            Query::where_item(column_name, "=", &value.to_sql_string(r#type, optional, dialect))
        }
    }

    fn where_entry(
        column_name: &str,
        field_type: &Type,
        optional: bool,
        value: &Value,
        dialect: SQLDialect,
    ) -> String {
        Self::where_entry_item(column_name, field_type, optional, value, dialect)
    }

    pub(crate) fn where_from_value(model: &Model, identifier: &Value, dialect: SQLDialect) -> String {
        let mut retval: Vec<String> = vec![];
        for (key, value) in identifier.as_dictionary().unwrap() {
            let field = model.field(key).unwrap();
            let column_name = field.column_name();
            let escape = dialect.escape();
            retval.push(format!("{}{}{} = {}", escape, column_name, escape, ToSQLString::to_string(&value, dialect)));
        }
        And(retval).to_string(dialect)
    }

    pub(crate) fn r#where(namespace: &Namespace, model: &Model, r#where: &Value, dialect: SQLDialect, table_alias: Option<&str>) -> String {
        let r#where = r#where.as_dictionary().unwrap();
        let mut retval: Vec<String> = vec![];
        for (key, value) in r#where.iter() {
            if key == "AND" {
                let inner = WhereClause::And(value.as_array().unwrap().iter().map(|w| Self::r#where(namespace, model, w, dialect, table_alias)).collect()).to_string(dialect);
                let val = "(".to_owned() + &inner + ")";
                retval.push(val);
            } else if key == "OR" {
                let inner = WhereClause::Or(value.as_array().unwrap().iter().map(|w| Self::r#where(namespace, model, w, dialect, table_alias)).collect()).to_string(dialect);
                let val = "(".to_owned() + &inner + ")";
                retval.push(val);
            } else if key == "NOT" {
                let inner = WhereClause::Not(Self::r#where(namespace, model, value, dialect, table_alias)).to_string(dialect);
                let val = "(".to_owned() + &inner + ")";
                retval.push(val);
            } else {
                if let Some(field) = model.field(key) {
                    let column_name = field.column_name();
                    let optional = field.optionality.is_any_optional();
                    let entry_column_name = if let Some(alias) = table_alias {
                        let _a = format!("{}.{}", alias, column_name);
                        Cow::Owned(format!("{}.{}", alias, column_name))
                    } else {
                        Cow::Borrowed(column_name)
                    };
                    let where_entry = Query::where_entry(&entry_column_name, field.r#type(), optional, value, dialect);
                    retval.push(where_entry);
                } else if let Some(relation) = model.relation(key) {
                    let has_join_table = relation.has_join_table();
                    let id_columns: Vec<&str> = model.primary_index().unwrap().keys().iter().map(|k| model.field(k).unwrap().column_name()).collect();
                    let id_columns_string = id_columns.iter().map(|k| k.escape(dialect)).collect::<Vec<String>>().join(",").to_wrapped();
                    let id_columns_prefixed_string = id_columns.iter().map(|s| format!("t.{}", s)).collect::<Vec<String>>();
                    let id_columns_prefixed = id_columns_prefixed_string.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
                    let _join_columns = if has_join_table {
                        let (_m, r) = namespace.through_relation(relation);
                        Some(r.references().iter().map(|k| model.field(k).unwrap().column_name()).collect::<Vec<&str>>())
                    } else { None };
                    let through_columns_string = if has_join_table {
                        let (m, r) = namespace.through_relation(relation);
                        r.fields().iter().map(|k| format!("t.{}", m.field(k).unwrap().column_name())).collect::<Vec<String>>()
                    } else { vec![] };
                    let through_columns = if has_join_table {
                        through_columns_string.iter().map(|k| k.as_str()).collect::<Vec<&str>>()
                    } else { vec![] };
                    for (key, value) in value.as_dictionary().unwrap() {
                        let escape = dialect.escape();
                        let from = if !has_join_table {
                            format!("{escape}{}{escape} AS t", &model.table_name)
                        } else {
                            let through_table_name = namespace.model_at_path(&relation.through_path().unwrap()).unwrap().table_name();
                            format!("{escape}{}{escape} AS t", through_table_name)
                        };
                        let opposite_model = namespace.model_at_path(&relation.model_path()).unwrap();
                        let relation_table_name = &opposite_model.table_name;
                        let on = if has_join_table {
                            let (_, opposite_relation) = namespace.opposite_relation(relation);
                            let opposite_relation = opposite_relation.unwrap();
                            let (join_model, join_relation) = namespace.through_relation(opposite_relation);
                            join_relation.iter().map(|(f, r)| {
                                let f = join_model.field(f).unwrap().column_name();
                                let r = opposite_model.field(r).unwrap().column_name();
                                format!("j.{} = t.{}", r.escape(dialect), f.escape(dialect))
                            }).collect::<Vec<String>>().join(",")
                        } else {
                            relation.iter().map(|(f, r)| {
                                let f = model.field(f).unwrap().column_name();
                                let r = opposite_model.field(r).unwrap().column_name();
                                format!("j.{} = t.{}", r.escape(dialect), f.escape(dialect))
                            }).collect::<Vec<String>>().join(",")
                        };
                        let addition_where = if has_join_table {
                            let (m, r) = namespace.through_relation(relation);
                            r.iter().map(|(f, _r)| {
                                let f = m.field(f).unwrap().column_name();
                                format!("t.{} IS NOT NULL", f.escape(dialect))
                            }).collect::<Vec<String>>().join(" AND ")
                        } else {
                            relation.iter().map(|(f, _r)| {
                                let f = model.field(f).unwrap().column_name();
                                format!("t.{} IS NOT NULL", f.escape(dialect))
                            }).collect::<Vec<String>>().join(" AND ")
                        };
                        let mut inner_where = Query::r#where(namespace, opposite_model, value, dialect, Some("j"));
                        if key.as_str() == "every" {
                            inner_where = Not(inner_where.to_wrapped()).to_string(dialect).to_wrapped();
                        }
                        if &inner_where == "" {
                            inner_where = addition_where
                        } else {
                            inner_where = And(vec![inner_where, addition_where]).to_string(dialect);
                        }
                        let inner_stmt = SQL::select(Some(if has_join_table { &through_columns } else { &id_columns_prefixed }), &from)
                            .inner_join(format!("{} AS j ON {}", relation_table_name.escape(dialect), on))
                            .r#where(inner_where).to_string(dialect).to_wrapped();
                        match key.as_str() {
                            "some" | "is" => {
                                retval.push(format!("{} IN {}", id_columns_string, inner_stmt))
                            }
                            "none" | "isNot" | "every" => {
                                retval.push(format!("{} NOT IN {}", id_columns_string, inner_stmt))
                            }
                            _ => panic!("Unhandled key.")
                        }
                    }

                }
            }
        }
        And(retval).to_string(dialect)
    }

    pub(crate) fn order_by(
        model: &Model,
        order_by: &Value,
        _dialect: SQLDialect,
        negative_take: bool,
    ) -> String {
        let asc = if negative_take { "DESC" } else { "ASC" };
        let desc = if negative_take { "ASC" } else { "DESC" };
        let order_by = order_by.as_array().unwrap();
        let mut retval: Vec<String> = vec![];
        for item in order_by.iter() {
            let (key, value) = Input::key_value(item.as_dictionary().unwrap());
            if let Some(field) = model.field(key) {
                let column_name = field.column_name();
                if let Some(str) = value.as_str() {
                    match str {
                        "asc" => retval.push(format!("{} {}", column_name, asc)),
                        "desc" => retval.push(format!("{} {}", column_name, desc)),
                        _ => panic!("Unhandled."),
                    }
                }
            }
        }
        retval.join(",")
    }

    pub(crate) fn build_for_count(
        namespace: &Namespace,
        model: &Model,
        value: &Value,
        dialect: SQLDialect,
        additional_where: Option<String>,
        additional_left_join: Option<String>,
        join_table_results: Option<Vec<String>>,
        force_negative_take: bool,
    ) -> Result<String> {
        Ok(format!("SELECT COUNT(*) FROM ({}) AS _", Self::build(namespace, model, value, dialect, additional_where, additional_left_join, join_table_results, force_negative_take)?))
    }

    pub(crate) fn build_for_group_by(
        namespace: &Namespace,
        model: &Model,
        value: &Value,
        dialect: SQLDialect,
    ) -> Result<String> {
        let aggregate = Self::build_for_aggregate(namespace, model, value, dialect)?;
        let map = value.as_dictionary().unwrap();
        let by = map.get("by").unwrap().as_array().unwrap().iter().map(|v| {
            let field_name = v.as_str().unwrap();
            model.field(field_name).unwrap().column_name()
        }).collect::<Vec<&str>>().join(",");
        let having = if let Some(having) = map.get("having") {
            let inner = Query::r#where(namespace, model, having, dialect, None);
            " HAVING (".to_owned() + &inner + ")"
        } else {
            "".to_owned()
        };
        Ok(format!("{} GROUP BY {}{}", aggregate, by, having))
    }

    pub(crate) fn build_for_aggregate(
        namespace: &Namespace,
        model: &Model,
        value: &Value,
        dialect: SQLDialect,
    ) -> Result<String> {
        let map = value.as_dictionary().unwrap();
        let escape = dialect.escape();
        let mut results: Vec<String> = vec![];
        for (key, value) in map {
            match key.as_str() {
                "_count" | "_sum" | "_avg" | "_min" | "_max" => {
                    for (k, v) in value.as_dictionary().unwrap() {
                        let k = k.as_str();
                        if v.as_bool().unwrap() {
                            match k {
                                "_all" => results.push(format!("COUNT(*) as {escape}_count._all{escape}")),
                                _ => {
                                    let column_name = model.field(k).unwrap().column_name();
                                    let func = SQL_AGGREGATE_MAP.get(key.as_str()).unwrap();
                                    // CAST(AVG(id) as DOUBLE)
                                    let mut left = format!("{}({})", func, column_name);
                                    match key.as_str() {
                                        "_avg" | "_sum" => left = format!("CAST({} AS DOUBLE)", left),
                                        _ => ()
                                    }
                                    results.push(format!("{} as {escape}{}.{}{escape}", left, key, k));
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        if let Some(by) = map.get("by") {
            for k in by.as_array().unwrap() {
                let field_name = k.as_enum_variant().unwrap();
                results.push(model.field(field_name.value.as_str()).unwrap().column_name().to_string());
            }
        }
        Ok(format!("SELECT {} FROM ({}) AS _", results.join(","), Self::build(namespace, model, value, dialect, None, None, None, false)?))
    }

    pub(crate) fn build(
        namespace: &Namespace,
        model: &Model,
        value: &Value,
        dialect: SQLDialect,
        additional_where: Option<String>,
        additional_left_join: Option<String>,
        join_table_results: Option<Vec<String>>,
        force_negative_take: bool,
    ) -> Result<String> {
        let r#where = value.get("where");
        let order_by = value.get("orderBy");
        let page_size = value.get("pageSize");
        let page_number = value.get("pageNumber");
        let skip = value.get("skip");
        let take = value.get("take");
        let cursor = value.get("cursor");
        let negative_take = if let Some(take) = take {
            take.as_int64().unwrap().is_negative()
        } else if force_negative_take {
            true
        } else {
            false
        };
        let table_name = if additional_left_join.is_some() {
            (model.table_name.to_string().escape(dialect) + " AS t")
        } else {
            model.table_name.to_string().escape(dialect)
        };
        let mut columns: Vec<String> = vec![];
        if additional_left_join.is_some() {
            columns = model.cache.save_keys.iter().map(|k| format!("t.{} AS {}", k.escape(dialect), k.escape(dialect))).collect::<Vec<String>>();
        }
        if let Some(join_table_results) = join_table_results {
            for result_key in join_table_results {
                columns.push(result_key);
            }
        }
        let column_refs = columns.iter().map(|c| c.as_str()).collect::<Vec<&str>>();
        let from = if let Some(cursor) = cursor {
            if order_by.is_none() {
                return Err(Error::new("cursor is invalid without order by argument"));
            }
            let order_by = order_by.unwrap().as_array().unwrap().get(0).unwrap().as_dictionary().unwrap();
            let key = order_by.keys().next().unwrap();
            let column_key = model.field(key).unwrap().column_name();
            let columns = cursor.as_dictionary().unwrap().keys().map(|_k| {
                if dialect == SQLDialect::PostgreSQL {
                    format!("{} AS \"c.{}\"", column_key, column_key)
                } else {
                    format!("{} AS `c.{}`", column_key, column_key)
                }
            }).collect::<Vec<String>>();
            let column_refs: Vec<&str> = columns.iter().map(|k| k.as_str()).collect();
            let sub_where = Query::r#where(namespace, model, cursor, dialect, None);
            let mut query = SQL::select(Some(&column_refs), &table_name);
            query.r#where(sub_where);
            format!("{}, ({}) AS c", &table_name, &query.to_string(dialect))
        } else {
            table_name.clone()
        };
        let mut stmt = SQL::select(if columns.is_empty() { None } else { Some(&column_refs) }, from.as_ref());
        if let Some(r#where) = r#where {
            if !r#where.as_dictionary().unwrap().is_empty() {
                stmt.r#where(Query::r#where(namespace, model, r#where, dialect, None));
            }
        }
        if let Some(additional_where) = additional_where {
            if stmt.r#where.is_some() {
                stmt.r#where(And(vec![stmt.r#where.as_ref().unwrap().clone(), additional_where.to_string()]).to_string(dialect));
            } else {
                stmt.r#where(additional_where.to_string());
            }
        }
        if cursor.is_some() {
            let order_by = order_by.unwrap().as_array().unwrap().get(0).unwrap().as_dictionary().unwrap();
            let key = order_by.keys().next().unwrap();
            let order = if order_by.values().next().unwrap().as_str().unwrap() == if negative_take { "desc" } else { "asc" }
                { ">=" } else { "<=" };
            let cursor_where = Query::where_item(&key, order, &format!("`c.{}`", key));
            if stmt.r#where.is_some() {
                stmt.r#where(And(vec![stmt.r#where.as_ref().unwrap().clone(), cursor_where]).to_string(dialect));
            } else {
                stmt.r#where(cursor_where);
            }
        }
        if let Some(additional_left_join) = additional_left_join {
            stmt.left_join(additional_left_join);
        }
        if let Some(order_bys) = order_by {
            stmt.order_by(Query::order_by(model, order_bys, dialect, negative_take));
        } else if negative_take {
            let val = Self::default_desc_order(model);
            stmt.order_by(Query::order_by(model, &val, dialect, false));
        }
        if page_size.is_some() && page_number.is_some() {
            let skip: u64 = ((page_number.unwrap().as_int64().unwrap() - 1) * page_size.unwrap().as_int64().unwrap()) as u64;
            let limit: u64 = page_size.unwrap().as_int64().unwrap() as u64;
            stmt.limit(limit, skip);
        } else if skip.is_some() || take.is_some() {
            let skip: u64 = if skip.is_some() { skip.unwrap().as_int64().unwrap() as u64 } else { 0 };
            if dialect == SQLDialect::MySQL {
                let limit: u64 = if take.is_some() { take.unwrap().as_int64().unwrap().abs() as u64 } else { 18446744073709551615 };
                stmt.limit(limit, skip);
            } else {
                let limit: u64 = if take.is_some() { take.unwrap().as_int64().unwrap().abs() as u64 } else { 9223372036854775806 };
                stmt.limit(limit, skip);
            }
        }
        let result = stmt.to_string(dialect);
        Ok(result)
    }

    fn default_desc_order(model: &Model) -> Value {
        let mut vec: Vec<Value> = vec![];
        for item in &model.primary_index().unwrap().items {
            vec.push(Value::Dictionary(indexmap!{item.field.clone() => Value::String("desc".to_string())}));
        }
        Value::Array(vec)
    }
}

static SQL_AGGREGATE_MAP: Lazy<BTreeMap<&str, &str>> = Lazy::new(|| {
    btreemap!{
        "_count" => "COUNT",
        "_sum" => "SUM",
        "_avg" => "AVG",
        "_min" => "MIN",
        "_max" => "MAX"
    }
});

pub(crate) fn escape_wisdom(s: impl AsRef<str>, dialect: SQLDialect) -> String {
    let s = s.as_ref();
    let escape = dialect.escape();
    if s.contains(escape) {
        s.to_owned()
    } else {
        s.split(".").map(|s| format!("{escape}{s}{escape}")).join(".")
    }
}