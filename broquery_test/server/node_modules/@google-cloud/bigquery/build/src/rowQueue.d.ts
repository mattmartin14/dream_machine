/*!
 * Copyright 2022 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { RequestCallback, Table, InsertStreamOptions } from '.';
import bigquery from './types';
import { RowBatch } from './rowBatch';
import { Stream } from 'stream';
import { RowBatchOptions, InsertRowsOptions, RowMetadata } from './table';
export interface MaxInsertOptions {
    maxOutstandingRows: number;
    maxOutstandingBytes: number;
    maxDelayMillis: number;
}
export declare const defaultOptions: MaxInsertOptions;
export type InsertRowsStreamResponse = bigquery.ITableDataInsertAllResponse;
export type InsertRowsCallback = RequestCallback<bigquery.ITableDataInsertAllResponse | bigquery.ITable>;
export interface InsertRow {
    insertId?: string;
    json?: bigquery.IJsonObject;
}
export type TableRow = bigquery.ITableRow;
export interface PartialInsertFailure {
    message: string;
    reason: string;
    row: RowMetadata;
}
/**
 * Standard row queue used for inserting rows.
 *
 *
 * @param {Table} table The table.
 * @param {Duplex} dup Row stream.
 * @param {InsertStreamOptions} options Insert and batch options.
 */
export declare class RowQueue {
    table: Table;
    stream: Stream;
    insertRowsOptions: InsertRowsOptions;
    batch: RowBatch;
    batchOptions?: RowBatchOptions;
    inFlight: boolean;
    pending?: ReturnType<typeof setTimeout>;
    constructor(table: Table, dup: Stream, options?: InsertStreamOptions);
    /**
     * Adds a row to the queue.
     *
     * @param {RowMetadata} row The row to insert.
     * @param {InsertRowsCallback} callback The insert callback.
     */
    add(row: RowMetadata, callback: InsertRowsCallback): void;
    /**
     * Cancels any pending inserts and calls _insert immediately.
     */
    insert(callback?: InsertRowsCallback): void;
    /**
     * Accepts a batch of rows and inserts them into table.
     *
     * @param {object[]} rows The rows to insert.
     * @param {InsertCallback[]} callbacks The corresponding callback functions.
     * @param {function} [callback] Callback to be fired when insert is done.
     */
    _insert(rows: RowMetadata | RowMetadata[], callbacks: InsertRowsCallback[], cb?: InsertRowsCallback): void;
    /**
     * Sets the batching options.
     *
     *
     * @param {RowBatchOptions} [options] The batching options.
     */
    setOptions(options?: RowBatchOptions): void;
    getOptionDefaults(): RowBatchOptions;
}
