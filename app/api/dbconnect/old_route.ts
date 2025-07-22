// app/api/db-connect.ts

import { NextRequest, NextResponse } from "next/server";
import { Pool } from "pg";
import mysql from "mysql2/promise";
import { Connector, IpAddressTypes } from "@google-cloud/cloud-sql-connector";


// const connectWithConnector = async (dbase: string) => {
//   const connector = new Connector();
//   const clientOpts = await connector.getOptions({
//     instanceConnectionName:
//       process.env[
//         `NEXT_PUBLIC_GCLOUDSQL_${dbase.toUpperCase()}_INSTANCE_CONNECTION_NAME`
//       ] || "",
//     ipType: "PUBLIC" as IpAddressTypes,
//   });

//   const dbConfig = {
//     ...clientOpts,
//     user: process.env[`NEXT_PUBLIC_GCLOUDSQL_${dbase.toUpperCase()}_DB_USER`],
//     password:
//       process.env[`NEXT_PUBLIC_GCLOUDSQL_${dbase.toUpperCase()}_DB_PASS`],
//     database:
//       process.env[`NEXT_PUBLIC_GCLOUDSQL_${dbase.toUpperCase()}_DB_NAME`],
//     // // host: `cloudsql:${process.env[`NEXT_PUBLIC_GCLOUDSQL_${dbase.toUpperCase()}_INSTANCE_CONNECTION_NAME`]}`,
//     // port: 5432,
//   };

//   return dbase === "PG" ? new Pool(dbConfig) : mysql.createPool(dbConfig);
// };

// const connectToPostgres = async () => {
//   const dbConfig = {
//     host: "database-1.cuiwtxtnfa4k.eu-north-1.rds.amazonaws.com", // Replace with your AWS RDS endpoint
//     port: 5432, // Default PostgreSQL port
//     user: "abhranshubagchi", // Replace with your master username
//     password: "India2020", // Replace with your password
//     database: "postgres", // Replace with your database name
//     ssl: {
//       // Configure SSL options to trust the self-signed certificate
//       // For example, you can set the 'rejectUnauthorized' option to 'false'
//       rejectUnauthorized: false
//     }
//   };

//   return new Pool(dbConfig);
// };

const connectToPostgres = async () => {
  const dbConfig = {
    host: "arjuna.db.elephantsql.com", // Replace with your 
    port: 5432, // Default PostgreSQL port
    user: "avygoulk", // Replace with your master username
    password: "nc2CieBjh75CF4rbe9iqYXVLQeCPCdcX", // Replace with your password
    database: "avygoulk", // Replace with your database name
    
  };

  return new Pool(dbConfig);
};


export async function POST(req: Request) {
  try {
    const body = await req.json();
    const { username, password, host, database, port, dbtype } = body;

    // console.log('*** POST REQUEST received *** = ', dbtype);

    let result: any;
    let sqlstr: string;
    let dbase: string;

    if (dbtype === "postgres" || dbtype === "gcloud") {
      dbase = "PG";
      const pool = new Pool({
        user: username,
        host: host,
        database: database,
        password: password,
        port: port,
        ssl: {
          // Configure SSL options to trust the self-signed certificate
          // For example, you can set the 'rejectUnauthorized' option to 'false'
          rejectUnauthorized: false
        }
      });

      sqlstr = `WITH table_info AS (
        SELECT
          cols.table_name,
          cols.column_name,
          cols.data_type,
          CASE
            WHEN pk.column_name IS NOT NULL THEN 'PRIMARY KEY'
            ELSE ''
          END AS key_type
        FROM information_schema.columns cols
        LEFT JOIN (
          SELECT
            tc.table_name,
            kcu.column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
        ) pk
        ON cols.table_name = pk.table_name
        AND cols.column_name = pk.column_name
        WHERE cols.table_schema = 'public'
        ORDER BY cols.table_name, cols.ordinal_position
      ),
      foreign_keys AS (
        SELECT
          kcu.table_name,
          kcu.column_name,
          ccu.table_name AS referenced_table_name
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.referential_constraints rc
          ON kcu.constraint_name = rc.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON rc.unique_constraint_name = ccu.constraint_name
        WHERE kcu.constraint_name IN (
          SELECT constraint_name
          FROM information_schema.table_constraints
          WHERE constraint_type = 'FOREIGN KEY'
            AND table_schema = 'public'
        )
      ),
      indexes AS (
        SELECT
          schemaname AS table_schema,
          tablename AS table_name,
          indexname AS index_name,
          indexdef AS index_definition
        FROM pg_indexes
        WHERE schemaname = 'public'
      )
      
      SELECT DISTINCT
        table_info.table_name,
        table_info.column_name,
        table_info.data_type,
        table_info.key_type,
        CASE
          WHEN foreign_keys.referenced_table_name IS NOT NULL THEN 'FOREIGN KEY'
          ELSE ''
        END AS foreign_key_type,
        CASE
          WHEN indexes.index_name IS NOT NULL THEN 'INDEX'
          ELSE ''
        END AS index_type,
        foreign_keys.referenced_table_name AS foreign_key_relation
      FROM table_info
      LEFT JOIN foreign_keys ON table_info.table_name = foreign_keys.table_name
        AND table_info.column_name = foreign_keys.column_name
      LEFT JOIN indexes ON table_info.table_name = indexes.table_name;`;

      const client = await pool.connect();

      result = await client?.query(sqlstr);

      client?.release();
      pool.end();
    } else if (dbtype === "mysql") {
      dbase = "MS";

      const connection = await mysql.createConnection({
        host: host,
        user: username,
        password: password,
        database: database,
        port: port,
      });

      sqlstr = `SELECT 
      cols.TABLE_NAME, 
      cols.COLUMN_NAME, 
      cols.DATA_TYPE, 
      CASE 
          WHEN kcu.COLUMN_NAME IS NOT NULL THEN 'PRIMARY KEY'
          ELSE ''
      END AS key_type, 
      CASE 
          WHEN kcu1.COLUMN_NAME IS NOT NULL THEN 'FOREIGN KEY'
          ELSE ''
      END AS foreign_key_type, 
      '' AS index_type, 
      kcu1.REFERENCED_TABLE_NAME AS foreign_key_relation 
  FROM INFORMATION_SCHEMA.COLUMNS cols 
  LEFT JOIN (
      SELECT 
          tc.TABLE_NAME, 
          kcu.COLUMN_NAME 
      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
      JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
          ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
          AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
  ) kcu ON cols.TABLE_NAME = kcu.TABLE_NAME 
  AND cols.COLUMN_NAME = kcu.COLUMN_NAME 
  LEFT JOIN (
      SELECT 
          kcu.TABLE_NAME, 
          kcu.COLUMN_NAME, 
          kcu.REFERENCED_TABLE_NAME 
      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
      JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc 
          ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
  ) kcu1 ON cols.TABLE_NAME = kcu1.TABLE_NAME 
  AND cols.COLUMN_NAME = kcu1.COLUMN_NAME;`;

      result = await connection.query(sqlstr);

      connection.end();
    } else if (dbtype === "gcloud") {
      dbase = "PG";
      sqlstr = `WITH table_info AS (
        SELECT
          cols.table_name,
          cols.column_name,
          cols.data_type,
          CASE
            WHEN pk.column_name IS NOT NULL THEN 'PRIMARY KEY'
            ELSE ''
          END AS key_type
        FROM information_schema.columns cols
        LEFT JOIN (
          SELECT
            tc.table_name,
            kcu.column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
        ) pk
        ON cols.table_name = pk.table_name
        AND cols.column_name = pk.column_name
        WHERE cols.table_schema = 'public'
        ORDER BY cols.table_name, cols.ordinal_position
      ),
      foreign_keys AS (
        SELECT
          kcu.table_name,
          kcu.column_name,
          ccu.table_name AS referenced_table_name
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.referential_constraints rc
          ON kcu.constraint_name = rc.constraint_name
        JOIN information_schema.constraint_column_usage ccu
          ON rc.unique_constraint_name = ccu.constraint_name
        WHERE kcu.constraint_name IN (
          SELECT constraint_name
          FROM information_schema.table_constraints
          WHERE constraint_type = 'FOREIGN KEY'
            AND table_schema = 'public'
        )
      ),
      indexes AS (
        SELECT
          schemaname AS table_schema,
          tablename AS table_name,
          indexname AS index_name,
          indexdef AS index_definition
        FROM pg_indexes
        WHERE schemaname = 'public'
      )
      
      SELECT DISTINCT
        table_info.table_name,
        table_info.column_name,
        table_info.data_type,
        table_info.key_type,
        CASE
          WHEN foreign_keys.referenced_table_name IS NOT NULL THEN 'FOREIGN KEY'
          ELSE ''
        END AS foreign_key_type,
        CASE
          WHEN indexes.index_name IS NOT NULL THEN 'INDEX'
          ELSE ''
        END AS index_type,
        foreign_keys.referenced_table_name AS foreign_key_relation
      FROM table_info
      LEFT JOIN foreign_keys ON table_info.table_name = foreign_keys.table_name
        AND table_info.column_name = foreign_keys.column_name
      LEFT JOIN indexes ON table_info.table_name = indexes.table_name;`;

      const pool = await connectToPostgres();
      const client = await pool.connect();

      result = await client?.query(sqlstr);

      client?.release();
      pool.end();
    } else {
      throw new Error("Unsupported database type");
    }

    return NextResponse.json(result);
  } catch (error) {
    console.error("db-connect route console logged 001:", error);
    return NextResponse.json({ message: error }, { status: 400 });
  }
}
