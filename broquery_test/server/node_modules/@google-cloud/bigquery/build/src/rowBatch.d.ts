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
import { InsertRowsCallback } from './rowQueue';
import { RowBatchOptions, RowMetadata } from './table';
export interface BatchLimits {
    maxBytes: number;
    maxRows: number;
}
export declare const BATCH_LIMITS: BatchLimits;
export interface InsertOptions {
    maxBytes?: number;
    maxRows?: number;
    maxMilliseconds?: number;
}
/**
 * Call used to help batch rows.
 *
 * @private
 *
 * @param {BatchInsertOptions} options The batching options.
 */
export declare class RowBatch {
    batchOptions: RowBatchOptions;
    rows: RowMetadata[];
    callbacks: InsertRowsCallback[];
    created: number;
    bytes: number;
    constructor(options: RowBatchOptions);
    /**
     * Adds a row to the current batch.
     *
     * @param {object} row The row to insert.
     * @param {InsertRowsCallback} callback The callback function.
     */
    add(row: RowMetadata, callback?: InsertRowsCallback): void;
    /**
     * Indicates if a given row can fit in the batch.
     *
     * @param {object} row The row in question.
     * @returns {boolean}
     */
    canFit(row: RowMetadata): boolean;
    /**
     * Checks to see if this batch is at the maximum allowed payload size.
     *
     * @returns {boolean}
     */
    isAtMax(): boolean;
    /**
     * Indicates if the batch is at capacity.
     *
     * @returns {boolean}
     */
    isFull(): boolean;
}
