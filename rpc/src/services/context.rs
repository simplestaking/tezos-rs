use crypto::hash::BlockHash;
use rusqlite::{named_params, Connection};
use serde::Serialize;
use std::collections::HashMap;

use tezos_timing::{
    hash_to_string, ActionData, ActionStats, ActionStatsWithRange, RangeStats, DB_PATH,
};

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BlockStats {
    actions_count: usize,
    tezedge_checkout_context_time: f64,
    tezedge_commit_context_time: f64,
    irmin_checkout_context_time: f64,
    irmin_commit_context_time: f64,
    operations_context: Vec<ActionStats>,
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ContextStats {
    commit_context: RangeStats,
    checkout_context: RangeStats,
    operations_context: Vec<ActionStatsWithRange>,
}

pub(crate) fn make_block_stats(block_hash: BlockHash) -> Result<BlockStats, failure::Error> {
    let sql = Connection::open(DB_PATH)?;
    make_block_stats_impl(&sql, block_hash)
}

pub(crate) fn make_context_stats(context_name: &str) -> Result<ContextStats, failure::Error> {
    let sql = Connection::open(DB_PATH)?;
    make_context_stats_impl(&sql, context_name)
}

fn make_context_stats_impl(
    sql: &Connection,
    context_name: &str,
) -> Result<ContextStats, failure::Error> {
    let mut stmt = sql.prepare(
        "
    SELECT
      action_name,
      root,
      one_to_ten_us_count,
      one_to_ten_us_mean_time,
      one_to_ten_us_max_time,
      one_to_ten_us_total_time,
      ten_to_one_hundred_us_count,
      ten_to_one_hundred_us_mean_time,
      ten_to_one_hundred_us_max_time,
      ten_to_one_hundred_us_total_time,
      one_hundred_us_to_one_ms_count,
      one_hundred_us_to_one_ms_mean_time,
      one_hundred_us_to_one_ms_max_time,
      one_hundred_us_to_one_ms_total_time,
      one_to_ten_ms_count,
      one_to_ten_ms_mean_time,
      one_to_ten_ms_max_time,
      one_to_ten_ms_total_time,
      ten_to_one_hundred_ms_count,
      ten_to_one_hundred_ms_mean_time,
      ten_to_one_hundred_ms_max_time,
      ten_to_one_hundred_ms_total_time,
      one_hundred_ms_to_one_s_count,
      one_hundred_ms_to_one_s_mean_time,
      one_hundred_ms_to_one_s_max_time,
      one_hundred_ms_to_one_s_total_time,
      one_to_ten_s_count,
      one_to_ten_s_mean_time,
      one_to_ten_s_max_time,
      one_to_ten_s_total_time,
      ten_to_one_hundred_s_count,
      ten_to_one_hundred_s_mean_time,
      ten_to_one_hundred_s_max_time,
      ten_to_one_hundred_s_total_time,
      one_hundred_s_count,
      one_hundred_s_mean_time,
      one_hundred_s_max_time,
      one_hundred_s_total_time,
      total_time,
      actions_count
    FROM
      global_action_stats
    WHERE
      context_name = :context_name
       ",
    )?;

    let mut rows = stmt.query(named_params! {
        ":context_name": context_name
    })?;

    let mut map: HashMap<String, ActionStatsWithRange> = HashMap::default();
    let mut commit_stats = RangeStats::default();
    let mut checkout_stats = RangeStats::default();

    while let Some(row) = rows.next()? {
        let action_name = match row.get_ref(0)?.as_str() {
            Ok(name) if !name.is_empty() => name,
            _ => continue,
        };

        let root = match row.get_ref(1)?.as_str() {
            Ok(root) if !root.is_empty() => root,
            _ => continue,
        };

        let mut action_stats = match action_name {
            "commit" => &mut commit_stats,
            "checkout" => &mut checkout_stats,
            _ => {
                let entry = match map.get_mut(root) {
                    Some(entry) => entry,
                    None => {
                        let mut stats = ActionStatsWithRange::default();
                        stats.root = root.to_string();
                        map.insert(root.to_string(), stats);
                        map.get_mut(root).unwrap()
                    }
                };
                match action_name {
                    "mem" => &mut entry.mem,
                    "find" => &mut entry.find,
                    "find_tree" => &mut entry.find_tree,
                    "add" => &mut entry.add,
                    "add_tree" => &mut entry.add_tree,
                    "remove" => &mut entry.remove,
                    _ => continue,
                }
            }
        };

        action_stats.one_to_ten_us.count = row.get(2)?;
        action_stats.one_to_ten_us.mean_time = row.get(3)?;
        action_stats.one_to_ten_us.max_time = row.get(4)?;
        action_stats.one_to_ten_us.total_time = row.get(5)?;
        action_stats.ten_to_one_hundred_us.count = row.get(6)?;
        action_stats.ten_to_one_hundred_us.mean_time = row.get(7)?;
        action_stats.ten_to_one_hundred_us.max_time = row.get(8)?;
        action_stats.ten_to_one_hundred_us.total_time = row.get(9)?;
        action_stats.one_hundred_us_to_one_ms.count = row.get(10)?;
        action_stats.one_hundred_us_to_one_ms.mean_time = row.get(11)?;
        action_stats.one_hundred_us_to_one_ms.max_time = row.get(12)?;
        action_stats.one_hundred_us_to_one_ms.total_time = row.get(13)?;
        action_stats.one_to_ten_ms.count = row.get(14)?;
        action_stats.one_to_ten_ms.mean_time = row.get(15)?;
        action_stats.one_to_ten_ms.max_time = row.get(16)?;
        action_stats.one_to_ten_ms.total_time = row.get(17)?;
        action_stats.ten_to_one_hundred_ms.count = row.get(18)?;
        action_stats.ten_to_one_hundred_ms.mean_time = row.get(19)?;
        action_stats.ten_to_one_hundred_ms.max_time = row.get(20)?;
        action_stats.ten_to_one_hundred_ms.total_time = row.get(21)?;
        action_stats.one_hundred_ms_to_one_s.count = row.get(22)?;
        action_stats.one_hundred_ms_to_one_s.mean_time = row.get(23)?;
        action_stats.one_hundred_ms_to_one_s.max_time = row.get(24)?;
        action_stats.one_hundred_ms_to_one_s.total_time = row.get(25)?;
        action_stats.one_to_ten_s.count = row.get(26)?;
        action_stats.one_to_ten_s.mean_time = row.get(27)?;
        action_stats.one_to_ten_s.max_time = row.get(28)?;
        action_stats.one_to_ten_s.total_time = row.get(29)?;
        action_stats.ten_to_one_hundred_s.count = row.get(30)?;
        action_stats.ten_to_one_hundred_s.mean_time = row.get(31)?;
        action_stats.ten_to_one_hundred_s.max_time = row.get(32)?;
        action_stats.ten_to_one_hundred_s.total_time = row.get(33)?;
        action_stats.one_hundred_s.count = row.get(34)?;
        action_stats.one_hundred_s.mean_time = row.get(35)?;
        action_stats.one_hundred_s.max_time = row.get(36)?;
        action_stats.one_hundred_s.total_time = row.get(37)?;
        action_stats.total_time = row.get(38)?;
        action_stats.actions_count = row.get(39)?;
    }

    let mut operations_context: Vec<_> = map
        .into_iter()
        .map(|(_, mut v)| {
            v.compute_total();
            v
        })
        .collect();

    operations_context.sort_by(|a, b| a.root.cmp(&b.root));

    Ok(ContextStats {
        operations_context,
        commit_context: commit_stats,
        checkout_context: checkout_stats,
    })
}

fn make_block_stats_impl(
    sql: &Connection,
    block_hash: BlockHash,
) -> Result<BlockStats, failure::Error> {
    let block_hash = hash_to_string(block_hash.as_ref());

    let (
        block_id,
        actions_count,
        tezedge_checkout_time,
        tezedge_commit_time,
        irmin_checkout_time,
        irmin_commit_time,
    ) = sql.query_row(
        "
    SELECT
      id,
      actions_count,
      checkout_time_tezedge,
      commit_time_tezedge,
      checkout_time_irmin,
      commit_time_irmin
    FROM
      blocks
    WHERE
      hash = ?1;
        ",
        [block_hash],
        |row| {
            Ok((
                row.get::<_, usize>(0)?,
                row.get::<_, usize>(1)?,
                row.get::<_, f64>(2)?,
                row.get::<_, f64>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, f64>(5)?,
            ))
        },
    )?;

    let mut stmt = sql
        .prepare(
            "
        SELECT
          root,
          actions_count,
          tezedge_mean_time,
          tezedge_max_time,
          tezedge_total_time,
          irmin_mean_time,
          irmin_max_time,
          irmin_total_time,
          tezedge_mem_time,
          tezedge_add_time,
          tezedge_add_tree_time,
          tezedge_find_time,
          tezedge_find_tree_time,
          tezedge_remove_time,
          irmin_mem_time,
          irmin_add_time,
          irmin_add_tree_time,
          irmin_find_time,
          irmin_find_tree_time,
          irmin_remove_time
        FROM
          block_action_stats
        WHERE
          block_id = ?;
        ",
        )
        .unwrap();

    let mut rows = stmt.query([block_id])?;

    let mut map: HashMap<String, ActionStats> = HashMap::default();

    while let Some(row) = rows.next()? {
        let root: String = match row.get(0) {
            Ok(root) => root,
            _ => continue,
        };

        let action_stats = ActionStats {
            data: ActionData {
                root: root.clone(),
                actions_count: row.get(1)?,
                tezedge_mean_time: row.get(2)?,
                tezedge_max_time: row.get(3)?,
                tezedge_total_time: row.get(4)?,
                irmin_mean_time: row.get(5)?,
                irmin_max_time: row.get(6)?,
                irmin_total_time: row.get(7)?,
            },
            tezedge_mem: row.get(8)?,
            tezedge_add: row.get(9)?,
            tezedge_add_tree: row.get(10)?,
            tezedge_find: row.get(11)?,
            tezedge_find_tree: row.get(12)?,
            tezedge_remove: row.get(13)?,
            irmin_mem: row.get(14)?,
            irmin_add: row.get(15)?,
            irmin_add_tree: row.get(16)?,
            irmin_find: row.get(17)?,
            irmin_find_tree: row.get(18)?,
            irmin_remove: row.get(19)?,
        };

        map.insert(root, action_stats);
    }

    let mut operations_context: Vec<_> = map.into_iter().map(|(_, v)| v).collect();
    operations_context.sort_by(|a, b| a.data.root.cmp(&b.data.root));

    Ok(BlockStats {
        actions_count,
        tezedge_checkout_context_time: tezedge_checkout_time,
        tezedge_commit_context_time: tezedge_commit_time,
        irmin_checkout_context_time: irmin_checkout_time,
        irmin_commit_context_time: irmin_commit_time,
        operations_context,
    })
}

#[cfg(test)]
mod tests {
    use crypto::hash::HashTrait;
    use rusqlite::Batch;

    use super::*;

    #[test]
    fn test_read_db() {
        let sql = Connection::open_in_memory().unwrap();

        let block_hash = BlockHash::try_from_bytes(&vec![1; 32]).unwrap();
        let block_hash_str = hash_to_string(block_hash.as_ref());

        let schema = include_str!("../../../tezos/timing/src/schema_stats.sql");
        let mut batch = Batch::new(&sql, schema);
        while let Some(mut stmt) = batch.next().unwrap() {
            stmt.execute([]).unwrap();
        }

        sql.execute(
            "
            INSERT INTO blocks
               (id, hash, actions_count, checkout_time_tezedge, commit_time_tezedge, checkout_time_irmin, commit_time_irmin)
            VALUES
               (1, ?1, 4, 10.0, 11.0, 12.0, 13.0);",
            [block_hash_str],
        )
        .unwrap();

        sql.execute(
            "
        INSERT INTO block_action_stats
          (root, block_id, actions_count, tezedge_mean_time, tezedge_max_time, tezedge_total_time,
           tezedge_mem_time, tezedge_find_time, tezedge_find_tree_time, tezedge_add_time, tezedge_add_tree_time, tezedge_remove_time,
           irmin_mean_time, irmin_max_time, irmin_total_time,
           irmin_mem_time, irmin_find_time, irmin_find_tree_time, irmin_add_time, irmin_add_tree_time, irmin_remove_time)
        VALUES
          ('a', 1, 40, 100.5, 3.0, 4.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
          ('m', 1, 400, 100.6, 30.0, 40.0, 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
            ",
            [],
        )
        .unwrap();

        let block_stats = make_block_stats_impl(&sql, block_hash).unwrap();

        assert_eq!(block_stats.actions_count, 4);
        assert_eq!(block_stats.tezedge_checkout_context_time, 10.0);
        assert_eq!(block_stats.tezedge_commit_context_time, 11.0);
        assert_eq!(block_stats.operations_context.len(), 2);

        let action = block_stats
            .operations_context
            .iter()
            .find(|a| a.data.root == "a")
            .unwrap();
        assert_eq!(action.data.root, "a");
        assert_eq!(action.data.tezedge_mean_time, 100.5);
        assert_eq!(action.tezedge_add, 1.4);
        assert_eq!(action.tezedge_find_tree, 1.3);

        sql.execute(
            "
        INSERT INTO global_action_stats
          (context_name, action_name, root, one_to_ten_us_count, one_to_ten_us_mean_time,
           one_to_ten_us_max_time, one_to_ten_us_total_time, actions_count, total_time)
        VALUES
          ('tezedge', 'mem', 'a', 2, 1.3, 1.4, 1.5, 1, 2.0),
          ('tezedge', 'mem', 'b', 3, 10.3, 10.4, 10.5, 1, 2.0),
          ('tezedge', 'add', 'b', 4, 20.3, 20.4, 20.5, 1, 2.0),
          ('tezedge', 'commit', 'commit', 4, 30.3, 30.4, 30.5, 1, 2.0),
          ('tezedge', 'checkout', 'checkout', 5, 40.3, 40.4, 40.5, 1, 2.0);
            ",
            [],
        )
        .unwrap();

        let context_stats = make_context_stats_impl(&sql, "tezedge").unwrap();

        assert_eq!(context_stats.operations_context.len(), 2);
        assert_eq!(context_stats.commit_context.one_to_ten_us.mean_time, 30.3);
        assert_eq!(context_stats.commit_context.actions_count, 1);
        assert_eq!(context_stats.checkout_context.one_to_ten_us.mean_time, 40.3);

        let action = context_stats
            .operations_context
            .iter()
            .find(|a| a.root == "a")
            .unwrap();
        assert_eq!(action.mem.one_to_ten_us.mean_time, 1.3);
        assert_eq!(action.mem.one_to_ten_us.count, 2);
        assert_eq!(action.total_time, 2.0);
        assert_eq!(action.mem.total_time, 2.0);

        let action = context_stats
            .operations_context
            .iter()
            .find(|a| a.root == "b")
            .unwrap();
        assert_eq!(action.mem.one_to_ten_us.mean_time, 10.3);
        assert_eq!(action.mem.one_to_ten_us.count, 3);
        assert_eq!(action.add.one_to_ten_us.mean_time, 20.3);
        assert_eq!(action.add.one_to_ten_us.count, 4);
    }
}
