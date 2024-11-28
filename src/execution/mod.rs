use std::borrow::Cow;
use array_tool::vec::Uniq;
use std::backtrace::Backtrace;
use std::collections::HashMap;
use async_recursion::async_recursion;
use indexmap::IndexMap;
use key_path::KeyPath;
use quaint_forked::prelude::{Queryable, ResultRow};
use quaint_forked::ast::{Query as QuaintQuery};
use teo_parser::r#type::Type;
use crate::query::Query;
use crate::schema::dialect::SQLDialect;
use crate::schema::value::decode::RowDecoder;
use crate::schema::value::encode::{SQLEscape, ToSQLString, ToWrapped};
use teo_runtime::action::Action;
use teo_runtime::connection::transaction;
use teo_runtime::model::field::column_named::ColumnNamed;
use teo_runtime::model::field::is_optional::IsOptional;
use teo_runtime::model::field::typed::Typed;
use teo_runtime::model::object::input::Input;
use teo_runtime::model::Model;
use teo_runtime::model::Object;
use teo_runtime::namespace::Namespace;
use teo_runtime::error_ext;
use teo_runtime::request;
use teo_runtime::request::Request;
use teo_runtime::traits::named::Named;
use teo_runtime::value::Value;
use teo_runtime::teon;

pub(crate) struct Execution { }

impl Execution {

    pub(crate) fn row_to_value(namespace: &Namespace, model: &Model, row: &ResultRow, columns: &Vec<String>, dialect: SQLDialect) -> Value {
        Value::Dictionary(columns.iter().filter_map(|column_name| {
            if let Some(field) = model.field_with_column_name(column_name) {
                if field.auto_increment() && dialect == SQLDialect::PostgreSQL {
                    Some((field.name().to_owned(), RowDecoder::decode_serial(field.is_optional(), row, column_name)))
                } else {
                    Some((field.name().to_owned(), RowDecoder::decode(field.r#type(), field.is_optional(), row, column_name, dialect)))
                }
            } else if let Some(property) = model.property_with_column_name(column_name) {
                Some((property.column_name().to_owned(), RowDecoder::decode(property.r#type(), property.is_optional(), row, column_name, dialect)))
            } else if column_name.contains(".") {
                let names: Vec<&str> = column_name.split(".").collect();
                let relation_name = names[0];
                let field_name = names[1];
                if relation_name == "c" { // cursor fetch, should remove
                    None
                } else {
                    let relation = model.relation(relation_name).unwrap();
                    let opposite_model = namespace.model_at_path(&relation.model_path()).unwrap();
                    let field = opposite_model.field(field_name).unwrap();
                    Some((column_name.to_owned(), RowDecoder::decode(field.r#type(), field.is_optional(), row, column_name, dialect)))
                }
            } else {
                panic!("Unhandled key {}.", column_name);
            }
        }).collect())
    }

    fn row_to_aggregate_value(model: &Model, row: &ResultRow, columns: &Vec<String>, dialect: SQLDialect) -> Value {
        let mut retval: IndexMap<String, Value> = IndexMap::new();
        for column in columns {
            let result_key = column.as_str();
            if result_key.contains(".") {
                let splitted = result_key.split(".").collect::<Vec<&str>>();
                let group = *splitted.get(0).unwrap();
                let field_name = *splitted.get(1).unwrap();
                if !retval.contains_key(group) {
                    retval.insert(group.to_string(), Value::Dictionary(IndexMap::new()));
                }
                if group == "_count" { // force i64
                    let count: i64 = row.get(result_key).unwrap().as_i64().unwrap();
                    retval.get_mut(group).unwrap().as_dictionary_mut().unwrap().insert(field_name.to_string(), teon!(count));
                } else if group == "_avg" || group == "_sum" { // force f64
                    let v = RowDecoder::decode(&Type::Float, true, &row, result_key, dialect);
                    retval.get_mut(group).unwrap().as_dictionary_mut().unwrap().insert(field_name.to_string(), v);
                } else { // field type
                    let field = model.field(field_name).unwrap();
                    let v = RowDecoder::decode(field.r#type(), true, &row, result_key, dialect);
                    retval.get_mut(group).unwrap().as_dictionary_mut().unwrap().insert(field_name.to_string(), v);
                }
            } else if let Some(field) = model.field_with_column_name(result_key) {
                retval.insert(field.name().to_owned(), RowDecoder::decode(field.r#type(), field.is_optional(), row, result_key, dialect));
            } else if let Some(property) = model.property(result_key) {
                retval.insert(property.name().to_owned(), RowDecoder::decode(property.r#type(), property.is_optional(), row, result_key, dialect));
            }
        }
        Value::Dictionary(retval)
    }

    pub(crate) async fn query_objects<'a>(namespace: &Namespace, conn: &'a dyn Queryable, model: &Model, finder: &'a Value, dialect: SQLDialect, action: Action, transaction_ctx: transaction::Ctx, request: Option<Request>, path: KeyPath) -> teo_result::Result<Vec<Object>> {
        let values = Self::query(namespace, conn, model, finder, dialect, path).await?;
        let select = finder.as_dictionary().unwrap().get("select");
        let include = finder.as_dictionary().unwrap().get("include");
        let mut results = vec![];
        for value in values {
            let object = transaction_ctx.new_object(model, action, request.clone())?;
            object.set_from_database_result_value(&value, select, include);
            results.push(object);
        }
        Ok(results)
    }

    #[async_recursion]
    async fn query_internal(namespace: &Namespace, conn: &dyn Queryable, model: &Model, value: &Value, dialect: SQLDialect, additional_where: Option<String>, additional_left_join: Option<String>, join_table_results: Option<Vec<String>>, force_negative_take: bool, additional_distinct: Option<Vec<String>>, path: KeyPath) -> teo_result::Result<Vec<Value>> {
        let _select = value.get("select");
        let include = value.get("include");
        let original_distinct = value.get("distinct").map(|v| if v.as_array().unwrap().is_empty() { None } else { Some(v.as_array().unwrap()) }).flatten();
        let distinct = Self::merge_distinct(original_distinct, additional_distinct);
        let skip = value.get("skip");
        let take = value.get("take");
        let should_in_memory_take_skip = distinct.is_some() && (skip.is_some() || take.is_some());
        let value_for_build = if should_in_memory_take_skip {
            Self::without_paging_and_skip_take(value)
        } else {
            Cow::Borrowed(value)
        };
        let stmt = Query::build(namespace, model, value_for_build.as_ref(), dialect, additional_where, additional_left_join, join_table_results, force_negative_take)?;
        // println!("see sql query stmt: {}", &stmt);
        let reverse = Input::has_negative_take(value);
        let rows = match conn.query(QuaintQuery::from(stmt)).await {
            Ok(rows) => rows,
            Err(err) => {
                return Err(error_ext::unknown_database_find_error(path.clone(), format!("{:?}", err)));
            }
        };
        if rows.is_empty() {
            return Ok(vec![])
        }
        let columns = rows.columns().clone();
        let mut results = rows.into_iter().map(|row| Self::row_to_value(namespace, model, &row, &columns, dialect)).collect::<Vec<Value>>();
        if reverse {
            results.reverse();
        }
        if let Some(distinct) = distinct {
            let distinct_keys = distinct.iter().map(|s| s.as_str()).collect::<Vec<&str>>();
            results = results.unique_via(|a, b| {
                Self::sub_hashmap(a, &distinct_keys) == Self::sub_hashmap(b, &distinct_keys)
            });
        }
        if should_in_memory_take_skip {
            let skip = skip.map(|s| s.as_int64().unwrap()).unwrap_or(0) as usize;
            let take = take.map(|s| s.as_int64().unwrap().abs() as u64).unwrap_or(0) as usize;
            results = results.into_iter().enumerate().filter(|(i, _r)| {
                *i >= skip && *i < (skip + take)
            }).map(|(_i, r)| r.clone()).collect();
            if reverse {
                results.reverse();
            }
        }
        if let Some(include) = include.map(|i| i.as_dictionary().unwrap()) {
            for (key, value) in include {
                let skip = value.as_dictionary().map(|m| m.get("skip")).flatten().map(|v| v.as_int64().unwrap());
                let take = value.as_dictionary().map(|m| m.get("take")).flatten().map(|v| v.as_int64().unwrap());
                let take_abs = take.map(|t| t.abs() as u64);
                let negative_take = take.map(|v| v.is_negative()).unwrap_or(false);
                let inner_distinct = value.as_dictionary().map(|m| m.get("distinct")).flatten().map(|v| if v.as_array().unwrap().is_empty() { None } else { Some(v.as_array().unwrap()) }).flatten();
                let relation = model.relation(key).unwrap();
                let (opposite_model, _) = namespace.opposite_relation(relation);
                if !relation.has_join_table() {
                    let fields = relation.fields();
                    let opposite_fields = relation.references();
                    let names = if opposite_fields.len() == 1 {
                        opposite_model.field(opposite_fields.get(0).unwrap()).unwrap().column_name().escape(dialect)
                    } else {
                        opposite_fields.iter().map(|f| opposite_model.field(f).unwrap().column_name().escape(dialect)).collect::<Vec<String>>().join(",").to_wrapped()
                    };
                    let values = if opposite_fields.len() == 1 {
                        // in a (?,?,?,?,?) format
                        let field_name = fields.get(0).unwrap();
                        results.iter().map(|v| {
                            ToSQLString::to_string(&v.as_dictionary().unwrap().get(field_name).unwrap(), dialect)
                        }).collect::<Vec<String>>().join(",").to_wrapped()
                    } else {
                        // in a (VALUES (?,?),(?,?)) format
                        format!("(VALUES {})", results.iter().map(|o| {
                            fields.iter().map(|f| ToSQLString::to_string(&o.as_dictionary().unwrap().get(f).unwrap(), dialect)).collect::<Vec<String>>().join(",").to_wrapped()
                        }).collect::<Vec<String>>().join(","))
                    };
                    let where_addition = Query::where_item(&names, "IN", &values);
                    let nested_query = if value.is_dictionary() {
                        Self::without_paging_and_skip_take_distinct(value)
                    } else {
                        Cow::Owned(teon!({}))
                    };
                    let included_values = Self::query_internal(namespace, conn, opposite_model, &nested_query, dialect, Some(where_addition), None, None, negative_take, None, path.clone()).await?;
                    // println!("see included: {:?}", included_values);
                    for result in results.iter_mut() {
                        let mut skipped = 0;
                        let mut taken = 0;
                        if relation.is_vec() {
                            result.as_dictionary_mut().unwrap().insert(relation.name().to_owned(), Value::Array(vec![]));
                        }
                        for included_value in included_values.iter() {
                            let mut matched = true;
                            for (field, reference) in relation.iter() {
                                if included_value.get(reference).is_none() && result.get(field).is_none() {
                                    matched = false;
                                    break;
                                }
                                if included_value.get(reference) != result.get(field) {
                                    matched = false;
                                    break;
                                }
                            }
                            if matched {
                                if (skip.is_none() || skip.unwrap() <= skipped) && (take.is_none() || taken < take_abs.unwrap()) {
                                    if result.get(relation.name()).is_none() {
                                        result.as_dictionary_mut().unwrap().insert(relation.name().to_owned(), Value::Array(vec![]));
                                    }
                                    if negative_take {
                                        result.as_dictionary_mut().unwrap().get_mut(relation.name()).unwrap().as_array_mut().unwrap().insert(0, included_value.clone());
                                    } else {
                                        result.as_dictionary_mut().unwrap().get_mut(relation.name()).unwrap().as_array_mut().unwrap().push(included_value.clone());
                                    }
                                    taken += 1;
                                    if take.is_some() && (taken >= take_abs.unwrap()) {
                                        break;
                                    }
                                } else {
                                    skipped += 1;
                                }
                            }
                        }
                    }
                } else {
                    let (opposite_model, opposite_relation) = namespace.opposite_relation(relation);
                    let (through_model, through_opposite_relation) = namespace.through_opposite_relation(relation);
                    let mut join_parts: Vec<String> = vec![];
                    for (field, reference) in through_opposite_relation.iter() {
                        let field_column_name = through_model.field(field).unwrap().column_name();
                        let reference_column_name = opposite_model.field(reference).unwrap().column_name();
                        join_parts.push(format!("t.{} = j.{}", reference_column_name.escape(dialect), field_column_name.escape(dialect)));
                    }
                    let joins = join_parts.join(" AND ");
                    let left_join = format!("{} AS j ON {}", &through_model.table_name().escape(dialect), joins);
                    let (through_table, through_relation) = namespace.through_relation(relation);
                    let names = if through_relation.len() == 1 { // todo: column name
                        format!("j.{}", through_table.field(through_relation.fields().get(0).unwrap()).unwrap().column_name().escape(dialect))
                    } else {
                        through_relation.fields().iter().map(|f| format!("j.{}", through_table.field(f).unwrap().column_name().escape(dialect))).collect::<Vec<String>>().join(",").to_wrapped()
                    };
                    let values = if through_relation.len() == 1 { // (?,?,?,?,?) format
                        let references = through_relation.references();
                        let field_name = references.get(0).unwrap();
                        results.iter().map(|v| {
                            ToSQLString::to_string(&v.as_dictionary().unwrap().get(field_name).unwrap(), dialect)
                        }).collect::<Vec<String>>().join(",").to_wrapped()
                    } else { // (VALUES (?,?),(?,?)) format
                        let pairs = results.iter().map(|o| {
                            through_relation.references().iter().map(|f| ToSQLString::to_string(&o.as_dictionary().unwrap().get(f).unwrap(), dialect)).collect::<Vec<String>>().join(",").to_wrapped()
                        }).collect::<Vec<String>>().join(",");
                        format!("(VALUES {})", pairs)
                    };
                    let where_addition = Query::where_item(&names, "IN", &values);
                    let nested_query = if value.is_dictionary() {
                        Self::without_paging_and_skip_take(value)
                    } else {
                        Cow::Owned(teon!({}))
                    };
                    let join_table_results = through_relation.iter().map(|(f, r)| {
                        let through_column_name = through_model.field(f).unwrap().column_name().to_string();
                        if dialect == SQLDialect::PostgreSQL {
                            format!("j.{} AS \"{}.{}\"", through_column_name.as_str().escape(dialect), opposite_relation.unwrap().name(), r)
                        } else {
                            format!("j.{} AS `{}.{}`", through_column_name, opposite_relation.unwrap().name(), r)
                        }
                    }).collect();
                    let additional_inner_distinct = if inner_distinct.is_some() {
                        Some(through_relation.iter().map(|(_f, r)| {
                            format!("{}.{}", opposite_relation.unwrap().name(), r)
                        }).collect())
                    } else {
                        None
                    };
                    let included_values = Self::query_internal(namespace, conn, opposite_model, &nested_query, dialect, Some(where_addition), Some(left_join), Some(join_table_results), negative_take, additional_inner_distinct, path.clone()).await?;
                    // println!("see included {:?}", included_values);
                    for result in results.iter_mut() {
                        result.as_dictionary_mut().unwrap().insert(relation.name().to_owned(), Value::Array(vec![]));
                        let mut skipped = 0;
                        let mut taken = 0;
                        for included_value in included_values.iter() {
                            let mut matched = true;
                            for (_field, reference) in through_relation.iter() {
                                let key = format!("{}.{}", opposite_relation.unwrap().name(), reference);
                                if result.get(reference).is_none() && included_value.get(&key).is_none() {
                                    matched = false;
                                    break;
                                }
                                if result.get(reference) != included_value.get(&key) {
                                    matched = false;
                                    break;
                                }
                            }
                            if matched {
                                if (skip.is_none() || skip.unwrap() <= skipped) && (take.is_none() || taken < take_abs.unwrap()) {
                                    if negative_take {
                                        result.as_dictionary_mut().unwrap().get_mut(relation.name()).unwrap().as_array_mut().unwrap().insert(0, included_value.clone());
                                    } else {
                                        result.as_dictionary_mut().unwrap().get_mut(relation.name()).unwrap().as_array_mut().unwrap().push(included_value.clone());
                                    }
                                    taken += 1;
                                    if take.is_some() && (taken >= take_abs.unwrap()) {
                                        break;
                                    }
                                } else {
                                    skipped += 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    pub(crate) async fn query(namespace: &Namespace, conn: &dyn Queryable, model: &Model, finder: &Value, dialect: SQLDialect, path: KeyPath) -> teo_result::Result<Vec<Value>> {
       Self::query_internal(namespace, conn, model, finder, dialect, None, None, None, false, None, path).await
    }

    pub(crate) async fn query_aggregate(namespace: &Namespace, conn: &dyn Queryable, model: &Model, finder: &Value, dialect: SQLDialect, path: KeyPath) -> teo_result::Result<Value> {
        let stmt = Query::build_for_aggregate(namespace, model, finder, dialect)?;
        match conn.query(QuaintQuery::from(&*stmt)).await {
            Ok(result_set) => {
                let columns = result_set.columns().clone();
                let result = result_set.into_iter().next().unwrap();
                Ok(Self::row_to_aggregate_value(model, &result, &columns, dialect))
            },
            Err(err) => {
                return Err(error_ext::unknown_database_find_error(path, format!("{:?}", err)));
            }
        }
    }

    pub(crate) async fn query_group_by(namespace: &Namespace, conn: &dyn Queryable, model: &Model, finder: &Value, dialect: SQLDialect, path: KeyPath) -> teo_result::Result<Vec<Value>> {
        let stmt = Query::build_for_group_by(namespace, model, finder, dialect)?;
        let rows = match conn.query(QuaintQuery::from(stmt)).await {
            Ok(rows) => rows,
            Err(err) => {
                return Err(error_ext::unknown_database_find_error(path.clone(), format!("{:?}", err)));
            }
        };
        let columns = rows.columns().clone();
        Ok(rows.into_iter().map(|r| {
            Self::row_to_aggregate_value(model, &r, &columns, dialect)
        }).collect::<Vec<Value>>())
    }

    pub(crate) async fn query_count(namespace: &Namespace, conn: &dyn Queryable, model: &Model, finder: &Value, dialect: SQLDialect, path: KeyPath) -> teo_result::Result<Value> {
        if finder.get("select").is_some() {
            Self::query_count_fields(namespace, conn, model, finder, dialect, path).await
        } else {
            let result = Self::query_count_objects(namespace, conn, model, finder, dialect, path).await?;
            Ok(Value::Int64(result as i64))
        }
    }

    pub(crate) async fn query_count_objects(namespace: &Namespace, conn: &dyn Queryable, model: &Model, finder: &Value, dialect: SQLDialect, path: KeyPath) -> teo_result::Result<usize> {
        let stmt = Query::build_for_count(namespace, model, finder, dialect, None, None, None, false)?;
        match conn.query(QuaintQuery::from(stmt)).await {
            Ok(result) => {
                let result = result.into_iter().next().unwrap();
                let count: i64 = result.into_iter().next().unwrap().as_i64().unwrap();
                Ok(count as usize)
            },
            Err(err) => {
                return Err(error_ext::unknown_database_find_error(path.clone(), format!("{:?}", err)));
            }
        }
    }

    pub(crate) async fn query_count_fields(namespace: &Namespace, conn: &dyn Queryable, model: &Model, finder: &Value, dialect: SQLDialect, path: KeyPath) -> teo_result::Result<Value> {
        let new_finder = Value::Dictionary(finder.as_dictionary().unwrap().iter().map(|(k, v)| {
            if k.as_str() == "select" {
                ("_count".to_owned(), v.clone())
            } else {
                (k.to_owned(), v.clone())
            }
        }).collect());
        let aggregate_value = Self::query_aggregate(namespace, conn, model, &new_finder, dialect, path).await?;
        Ok(aggregate_value.get("_count").unwrap().clone())
    }

    fn without_paging_and_skip_take(value: &Value) -> Cow<Value> {
        let map = value.as_dictionary().unwrap();
        if map.contains_key("take") || map.contains_key("skip") || map.contains_key("pageSize") || map.contains_key("pageNumber") {
            let mut map = map.clone();
            map.remove("take");
            map.remove("skip");
            map.remove("pageSize");
            map.remove("pageNumber");
            Cow::Owned(Value::Dictionary(map))
        } else {
            Cow::Borrowed(value)
        }
    }

    fn without_paging_and_skip_take_distinct(value: &Value) -> Cow<Value> {
        let map = value.as_dictionary().unwrap();
        if map.contains_key("take") || map.contains_key("skip") || map.contains_key("pageSize") || map.contains_key("pageNumber") {
            let mut map = map.clone();
            map.remove("take");
            map.remove("skip");
            map.remove("pageSize");
            map.remove("pageNumber");
            map.remove("distinct");
            Cow::Owned(Value::Dictionary(map))
        } else {
            Cow::Borrowed(value)
        }
    }

    fn sub_hashmap(value: &Value, keys: &Vec<&str>) -> HashMap<String, Value> {
        let map = value.as_dictionary().unwrap();
        let mut retval = HashMap::new();
        for key in keys {
            retval.insert(key.to_string(), map.get(*key).cloned().unwrap_or(Value::Null));
        }
        retval
    }

    fn merge_distinct(value1: Option<&Vec<Value>>, value2: Option<Vec<String>>) -> Option<Vec<String>> {
        let mut result: Vec<String> = vec![];
        if let Some(value1) = value1 {
            for v in value1 {
                result.push(v.as_str().unwrap().to_string());
            }
        }
        if let Some(value2) = value2 {
            for v in value2 {
                result.push(v.to_string())
            }
        }
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}
