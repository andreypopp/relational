/**
 * @copyright 2015, Andrey Popp <me@andreypopp.com>
 */

import Promise from 'bluebird';
import sql from 'sql';
import pg from 'pg';

let pgClass = sql.define({
  name: 'pg_class',
  columns: [
    'oid',
    'relname',
    'relnamespace',
    'reltype',
    'reloftype',
    'relowner',
    'relam',
    'relfilenode',
    'reltablespace',
    'relpages',
    'reltuples',
    'relallvisible',
    'reltoastrelid',
    'relhasindex',
    'relisshared',
    'relpersistence',
    'relkind',
    'relnatts',
    'relchecks',
    'relhasoids',
    'relhaspkey',
    'relhasrules',
    'relhastriggers',
    'relhassubclass',
    'relispopulated',
    'relreplident',
    'relfrozenxid',
    'relminmxid',
    'relacl',
    'reloptions'
  ]
});

let pgAttribute = sql.define({
  name: 'pg_attribute',
  columns: [
    'oid',
    'attrelid',
    'attname',
    'atttypid',
    'attstattarget',
    'attlen',
    'attnum',
    'attndims',
    'attcacheoff',
    'atttypmod',
    'attbyval',
    'attstorage',
    'attalign',
    'attnotnull',
    'atthasdef',
    'attisdropped',
    'attislocal',
    'attinhcount',
    'attcollation',
    'attacl',
    'attoptions',
    'attfdwoptions'
  ]
});

let pgConstraint = sql.define({
  name: 'pg_constraint',
  columns: [
    'oid',
    'conname',
    'connamespace',
    'contype',
    'condeferrable',
    'condeferred',
    'convalidated',
    'conrelid',
    'contypid',
    'conindid',
    'confrelid',
    'confupdtype',
    'confdeltype',
    'confmatchtype',
    'conislocal',
    'coninhcount',
    'connoinherit',
    'conkey',
    'confkey',
    'conpfeqop',
    'conppeqop',
    'conffeqop',
    'conexclop',
    'conbin',
    'consrc'
  ]
});

let pgNamespace = sql.define({
  name: 'pg_namespace',
  columns: [
    'oid',
    'nspname',
    'nspowner',
    'nspacl'
  ]
});

let jsonAgg = sql.functionCallCreator('json_agg');
let unnest = sql.functionCallCreator('unnest');

/**
 * Connect to a PostgreSQL database.
 */
function connect(connString) {
  return new Promise((resolve, reject) => {
    let client = new pg.Client(connString);
    client.connect((err) => {
      if (err) {
        reject(err);
      } else {
        resolve(Promise.promisifyAll(client));
      }
    });
  });
}

/**
 * Get table definitions from PostgreSQL system catalog.
 */
async function getTables(client, schema = null) {
  let query = (
    pgClass
    .select(
      pgClass.oid,
      pgClass.relname,
      pgNamespace.nspname)
    .from(
      pgClass
      .join(pgNamespace)
      .on(pgClass.relnamespace.equals(pgNamespace.oid)))
    .where(pgClass.relkind.equals('r'))
  );
  if (schema !== null) {
    query = query.where(pgNamespace.nspname.equals(schema));
  }
  let {rows} = await client.queryAsync(query.toQuery());
  return rows;
}

/**
 * Get column definitions from PostgreSQL system catalog.
 */
async function getColumns(client) {
  let {rows} = await client.queryAsync(
    pgClass
      .select(pgClass.oid, jsonAgg(pgAttribute).as('columns'))
      .from(
        pgClass
        .join(pgAttribute)
        .on(pgAttribute.attrelid.equals(pgClass.oid)))
      .group(pgClass.oid)
      .toQuery());
  return arrayToObject(rows, 'oid');
}

async function getPrimaryKeys(client) {
  let query = `
    WITH c AS (
      SELECT conrelid, unnest(conkey) AS conkey
      FROM pg_catalog.pg_constraint
      WHERE contype = 'p')
    SELECT
      c.conrelid,
      json_agg(a.attname) as attname
    FROM pg_catalog.pg_attribute a
    JOIN c ON a.attrelid = c.conrelid AND a.attnum = c.conkey
    GROUP BY c.conrelid
  `;
  let {rows} = await client.queryAsync(query);
  return arrayToObject(rows, 'conrelid');
}

const NO_PRIMARY_KEYS = {attname: []};

/**
 * Reflect a database.
 */
async function reflectDatabase(connString, schema = null) {
  let client = await connect(connString);
  try {
    let tables = await getTables(client, schema);
    let columns = await getColumns(client);
    let primaryKeys = await getPrimaryKeys(client);
    let result = {};
    tables.forEach(table => {
      result[table.relname] = sql.define({
        name: table.relname,
        schema: table.nspname,
        columns: columns[table.oid].columns.map(column => ({
          name: column.attname,
          primaryKey: (primaryKeys[table.oid] || NO_PRIMARY_KEYS).attname.indexOf(column.attname) > -1
        }))
      });
    });
    return result;
  } finally {
    client.end();
  }
}

/**
 * Convert an array to an object by using a `key` as a key in object.
 */
function arrayToObject(rows, key = 'id') {
  var mapping = {};
  rows.forEach(row => {
    mapping[row[key]] = row;
  });
  return mapping;
}

function invariant(cond) {
  if (!cond) {
    throw new Error('invariant violation');
  }
}

function compile(metadata, spec) {
  invariant(spec.entity);
  let table = metadata[spec.entity];
  invariant(table);
  let query = table;
  if (spec.select) {
    query = query.select(
      table.columns
        .filter(column => spec.select[column.name])
        .map(column => {
          let columnSpec = spec.select[column.name];
          if (typeof columnSpec === 'string') {
            column = column.as(columnSpec);
          }
          return column;
        }),
      _synthesizeIDColumn(table, spec),
      _synthesizeEntityColumn(table, spec)
    );
  } else {
    query = query.select(
      table.columns,
      _synthesizeIDColumn(table, spec),
      _synthesizeEntityColumn(table, spec)
    );
  }
  return query;
}

function _synthesizeEntityColumn(table, spec) {
  return table.literal("'" + spec.entity + "'").as('$entity');
}

function _synthesizeIDColumn(table, spec) {
  let primaryKeys = table.columns
    .filter(column => column.primaryKey)
    .map(column => table[column.name].toQuery().text)
    .join('||');
  return table.literal(primaryKeys).as('$id');
}

async function main() {
  let connString = 'postgres://localhost/study_demo';
  let metadata = await reflectDatabase(connString);
  let query = compile(metadata, {
    entity: 'study',
    select: {
      code: true,
      closed: true
    }
  });
  let client = await connect(connString);
  try {
    console.log(query.toQuery());
    let {rows} = await client.queryAsync(query.toQuery());
    console.log(rows);
  } finally {
    client.end();
  }
}

main().catch(function(err) { console.log(err); });
