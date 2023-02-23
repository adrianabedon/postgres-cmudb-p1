// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "nlohmann/json.hpp"

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/optimizer.h"
#include "access/relation.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"
#include "access/tupmacs.h"
#include "utils/builtins.h"
#include <sys/stat.h>
}

#define METADATA_SIZE_OFFSET 4
#define STR_SIZE 32
#define INT_FLOAT_SIZE 4

// clang-format on
using json = nlohmann::json;

typedef struct BlockBuffer
{
  Datum *values;
  char *valueBuffer;
} BlockBuffer;

typedef struct TableReadState
{
  FILE *tableFile;
  json metadata;
  TupleDesc tupleDescriptor;
  /*
   * List of Var pointers for columns in the query. We use this both for
   * getting vector of projected columns, and also when we want to build
   * base constraint to find selected row blocks.
   */
  List *projectedColumnList;

  List *whereClauseList;

  BlockBuffer *blockBuffers;
  uint32_t blockBufferCount;
  uint32_t blockIndex;
  uint32_t blockSize;
  uint32_t blockNext;

  uint32_t overallRow;
} TableReadState;

static char *
db721_GetOptionValue(Oid foreignTableId, const char *optionName)
{
  ForeignTable *foreignTable = NULL;
  ForeignServer *foreignServer = NULL;
  List *optionList = NIL;
  ListCell *optionCell = NULL;
  char *optionValue = NULL;

  foreignTable = GetForeignTable(foreignTableId);
  foreignServer = GetForeignServer(foreignTable->serverid);

  optionList = list_concat(optionList, foreignTable->options);
  optionList = list_concat(optionList, foreignServer->options);

  foreach (optionCell, optionList)
  {
    DefElem *optionDef = (DefElem *)lfirst(optionCell);
    char *optionDefName = optionDef->defname;
    if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0)
    {
      optionValue = defGetString(optionDef);
      break;
    }
  }

  return optionValue;
}

/* Reads the given segment from the given file. */
static StringInfo
ReadFromFile(FILE *file, uint64 offset, uint32 size)
{
  int fseekResult = 0;
  int freadResult = 0;
  int fileError = 0;

  StringInfo resultBuffer = makeStringInfo();
  enlargeStringInfo(resultBuffer, size);
  resultBuffer->len = size;

  if (size == 0)
  {
    return resultBuffer;
  }

  errno = 0;
  fseekResult = fseeko(file, offset, SEEK_SET);
  if (fseekResult != 0)
  {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not seek in file: %m")));
  }

  freadResult = fread(resultBuffer->data, size, 1, file);
  if (freadResult != 1)
  {
    ereport(ERROR, (errmsg("could not read enough data from file")));
  }

  fileError = ferror(file);
  if (fileError != 0)
  {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not read file: %m")));
  }

  return resultBuffer;
}

static char *GetFileName(Oid foreigntableid)
{
  char *filename = db721_GetOptionValue(foreigntableid, "filename");
  if (filename == NULL)
  {
    ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                    errmsg("db721_fdw: missing filename option")));
  }
  return filename;
}

static json GetMetadata(FILE *tableFile, const char *filename)
{
  uint32_t metadataSize;
  StringInfo metadataSizeBytes;
  StringInfo metadataBytes;
  std::string metadataString;
  json metadata;
  struct stat st;

  if (tableFile == NULL)
  {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open file \"%s\" for reading: %m", filename)));
  }

  if (stat(filename, &st) < 0)
  {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not stat file \"%s\": %m", filename)));
  }

  metadataSizeBytes = ReadFromFile(tableFile, st.st_size - METADATA_SIZE_OFFSET, METADATA_SIZE_OFFSET);
  metadataSize = *((uint32_t *)metadataSizeBytes->data);

  if (tableFile == NULL)
  {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open file \"%s\" for reading: %m", filename)));
  }

  metadataBytes = ReadFromFile(tableFile, st.st_size - METADATA_SIZE_OFFSET - metadataSize, metadataSize);

  metadataString.assign(metadataBytes->data, metadataSize);
  metadata = json::parse(metadataString);

  pfree(metadataSizeBytes->data);
  pfree(metadataSizeBytes);
  pfree(metadataBytes->data);
  pfree(metadataBytes);

  return metadata;
}

static uint32_t GetEstRowCount(char *filename)
{
  FILE *tableFile = AllocateFile(filename, PG_BINARY_R);
  json metadata = GetMetadata(tableFile, filename);

  int values_per_block = metadata["Max Values Per Block"];
  json columns = metadata["Columns"];
  uint32_t row_count = 0;
  for (auto &column : columns)
  {
    int num_blocks = column["num_blocks"];
    row_count = num_blocks * values_per_block;
    break;
  }

  FreeFile(tableFile);

  return row_count;
}

/*
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list. This function is taken from mongo_fdw with
 * slight modifications.
 */
static List *
ColumnList(RelOptInfo *baserel, Oid foreignTableId)
{
  List *columnList = NIL;
  List *neededColumnList = NIL;
  AttrNumber columnIndex = 1;
  AttrNumber columnCount = baserel->max_attr;
#if PG_VERSION_NUM >= 90600
  List *targetColumnList = baserel->reltarget->exprs;
#else
  List *targetColumnList = baserel->reltargetlist;
#endif
  ListCell *targetColumnCell = NULL;
  List *restrictInfoList = baserel->baserestrictinfo;
  ListCell *restrictInfoCell = NULL;
  const AttrNumber wholeRow = 0;
  Relation relation = relation_open(foreignTableId, AccessShareLock);
  TupleDesc tupleDescriptor = RelationGetDescr(relation);

  /* first add the columns used in joins and projections */
  foreach (targetColumnCell, targetColumnList)
  {
    List *targetVarList = NIL;
    Node *targetExpr = (Node *)lfirst(targetColumnCell);

#if PG_VERSION_NUM >= 90600
    targetVarList = pull_var_clause(targetExpr,
                                    PVC_RECURSE_AGGREGATES |
                                        PVC_RECURSE_PLACEHOLDERS);
#else
    targetVarList = pull_var_clause(targetExpr,
                                    PVC_RECURSE_AGGREGATES,
                                    PVC_RECURSE_PLACEHOLDERS);
#endif

    neededColumnList = list_union(neededColumnList, targetVarList);
  }

  /* then walk over all restriction clauses, and pull up any used columns */
  foreach (restrictInfoCell, restrictInfoList)
  {
    RestrictInfo *restrictInfo = (RestrictInfo *)lfirst(restrictInfoCell);
    Node *restrictClause = (Node *)restrictInfo->clause;
    List *clauseColumnList = NIL;

    /* recursively pull up any columns used in the restriction clause */
#if PG_VERSION_NUM >= 90600
    clauseColumnList = pull_var_clause(restrictClause,
                                       PVC_RECURSE_AGGREGATES |
                                           PVC_RECURSE_PLACEHOLDERS);
#else
    clauseColumnList = pull_var_clause(restrictClause,
                                       PVC_RECURSE_AGGREGATES,
                                       PVC_RECURSE_PLACEHOLDERS);
#endif

    neededColumnList = list_union(neededColumnList, clauseColumnList);
  }

  /* walk over all column definitions, and de-duplicate column list */
  for (columnIndex = 1; columnIndex <= columnCount; columnIndex++)
  {
    ListCell *neededColumnCell = NULL;
    Var *column = NULL;
    Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, columnIndex - 1);

    if (attributeForm->attisdropped)
    {
      continue;
    }

    /* look for this column in the needed column list */
    foreach (neededColumnCell, neededColumnList)
    {
      Var *neededColumn = (Var *)lfirst(neededColumnCell);
      if (neededColumn->varattno == columnIndex)
      {
        column = neededColumn;
        break;
      }
      else if (neededColumn->varattno == wholeRow)
      {
        Index tableId = neededColumn->varno;

        column = makeVar(tableId, columnIndex, attributeForm->atttypid,
                         attributeForm->atttypmod, attributeForm->attcollation,
                         0);
        break;
      }
    }

    if (column != NULL)
    {
      columnList = lappend(columnList, column);
    }
  }

  relation_close(relation, AccessShareLock);

  return columnList;
}

TableReadState *
db721_BeginRead(const char *filename, TupleDesc tupleDescriptor,
                List *projectedColumnList, List *whereClauseList)
{
  TableReadState *readstate;
  FILE *tableFile = NULL;
  tableFile = AllocateFile(filename, PG_BINARY_R);
  if (tableFile == NULL)
  {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open file \"%s\" for reading: %m",
                           filename)));
  }

  readstate = (TableReadState *)palloc0(sizeof(TableReadState));
  readstate->tableFile = tableFile;
  readstate->metadata = GetMetadata(tableFile, filename);
  readstate->tupleDescriptor = tupleDescriptor;
  readstate->projectedColumnList = projectedColumnList;
  readstate->whereClauseList = whereClauseList;

  readstate->blockBufferCount = tupleDescriptor->natts;
  readstate->blockBuffers = (BlockBuffer *)palloc0(
      readstate->blockBufferCount * sizeof(BlockBuffer));

  uint32_t max_block_size = readstate->metadata["Max Values Per Block"].get<uint32_t>();

  ListCell *projectedColumnCell = NULL;
  foreach (projectedColumnCell, projectedColumnList)
  {
    Var *projectedColumn = (Var *)lfirst(projectedColumnCell);
    uint32 projectedColumnIndex = projectedColumn->varattno - 1;
    Form_pg_attribute attr = TupleDescAttr(tupleDescriptor, projectedColumnIndex);
    char *attr_name = NameStr(attr->attname);

    json column_metadata = readstate->metadata["Columns"][attr_name];
    std::string attr_type = column_metadata["type"].get<std::string>();

    readstate->blockBuffers[projectedColumnIndex].values = (Datum *)palloc(max_block_size * sizeof(Datum));

    if (attr_type == "str")
    {
      readstate->blockBuffers[projectedColumnIndex].valueBuffer = (char *)palloc(max_block_size * (STR_SIZE + VARHDRSZ));
    }
    else
    {
      readstate->blockBuffers[projectedColumnIndex].valueBuffer = NULL;
    }
  }

  readstate->blockIndex = UINT32_MAX;
  readstate->blockSize = 0;
  readstate->blockNext = 0;

  readstate->overallRow = 0;

  return readstate;
}

void db721_EndRead(TableReadState *readstate)
{
  FreeFile(readstate->tableFile);

  // ListCell *projectedColumnCell = NULL;
  // foreach (projectedColumnCell, readstate->projectedColumnList)
  // {
  //   Var *projectedColumn = (Var *)lfirst(projectedColumnCell);
  //   uint32 projectedColumnIndex = projectedColumn->varattno - 1;
  //   if (readstate->blockBuffers[projectedColumnIndex].values)
  //   {
  //     elog(LOG, "freeing values %d", projectedColumnIndex);
  //     free(readstate->blockBuffers[projectedColumnIndex].values);
  //   }
  //   if (readstate->blockBuffers[projectedColumnIndex].valueBuffer)
  //   {
  //     elog(LOG, "freeing value buffer %d", projectedColumnIndex);
  //     free(readstate->blockBuffers[projectedColumnIndex].valueBuffer);
  //   }
  // }
  // pfree(readstate->blockBuffers);

  // pfree(readstate);
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                        Oid foreigntableid)
{
  char *filename = GetFileName(foreigntableid);
  FILE *tableFile = AllocateFile(filename, PG_BINARY_R);
  if (tableFile == NULL)
  {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not open file \"%s\" for reading: %m", filename)));
  }

  baserel->rows = GetEstRowCount(filename);

  FreeFile(tableFile);
}

extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid)
{
  add_path(baserel,
           (Path *)create_foreignscan_path(
               root, baserel,
               NULL, /* path target */
               baserel->rows,
               0.0, 0.0,
               NIL,   /* no known ordering */
               NULL,  /* not parameterized */
               NULL,  /* no outer path */
               NIL)); /* no fdw_private */
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                     ForeignPath *best_path, List *tlist, List *scan_clauses,
                     Plan *outer_plan)
{
  ForeignScan *foreignScan = NULL;
  List *columnList = NIL;
  List *foreignPrivateList = NIL;

  /*
   * Although we skip row blocks that are refuted by the WHERE clause, but
   * we have no native ability to evaluate restriction clauses and make sure
   * that all non-related rows are filtered out. So we just put all of the
   * scanClauses into the plan node's qual list for the executor to check.
   */
  scan_clauses = extract_actual_clauses(scan_clauses,
                                        false); /* extract regular clauses */

  /*
   * As an optimization, we only read columns that are present in the query.
   * To find these columns, we need baserel. We don't have access to baserel
   * in executor's callback functions, so we get the column list here and put
   * it into foreign scan node's private list.
   */
  columnList = ColumnList(baserel, foreigntableid);
  foreignPrivateList = list_make1(columnList);

  /* create the foreign scan node */
  foreignScan = make_foreignscan(tlist, scan_clauses, baserel->relid,
                                 NIL, /* no expressions to evaluate */
                                 foreignPrivateList,
                                 NIL,
                                 NIL,
                                 NULL); /* no outer path */

  return foreignScan;
}

extern "C" void db721_BeginForeignScan(ForeignScanState *scanState, int eflags)
{
  TableReadState *readState = NULL;
  Oid foreignTableId = InvalidOid;
  Relation currentRelation = scanState->ss.ss_currentRelation;
  TupleDesc tupleDescriptor = RelationGetDescr(currentRelation);
  List *columnList = NIL;
  ForeignScan *foreignScan = NULL;
  List *foreignPrivateList = NIL;
  List *whereClauseList = NIL;

  /* if Explain with no Analyze, do nothing */
  if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
  {
    return;
  }

  foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);

  foreignScan = (ForeignScan *)scanState->ss.ps.plan;
  foreignPrivateList = (List *)foreignScan->fdw_private;
  whereClauseList = foreignScan->scan.plan.qual;

  columnList = (List *)linitial(foreignPrivateList);

  readState = db721_BeginRead(GetFileName(foreignTableId), tupleDescriptor,
                              columnList, whereClauseList);

  scanState->fdw_state = (void *)readState;
}

bool db721_ReadNextRow(TableReadState *readState, Datum *columnValues, bool *columnNulls)
{
  ListCell *projectedColumnCell = NULL;
  foreach (projectedColumnCell, readState->projectedColumnList)
  {
    Var *projectedColumn = (Var *)lfirst(projectedColumnCell);
    uint32 projectedColumnIndex = projectedColumn->varattno - 1;
    columnValues[projectedColumnIndex] = readState->blockBuffers[projectedColumnIndex].values[readState->blockNext];
    columnNulls[projectedColumnIndex] = false;
  }

  readState->blockNext++;
  readState->overallRow++;

  return true;
}

void fetch_more_data(ForeignScanState *scanState)
{
  TableReadState *readState = (TableReadState *)scanState->fdw_state;
  assert(readState->blockNext >= readState->blockSize);
  ListCell *projectedColumnCell = NULL;
  readState->blockIndex = readState->blockIndex == UINT32_MAX ? 0 : readState->blockIndex + 1;

  foreach (projectedColumnCell, readState->projectedColumnList)
  {

    Var *projectedColumn = (Var *)lfirst(projectedColumnCell);
    uint32 projectedColumnIndex = projectedColumn->varattno - 1;
    Form_pg_attribute attr = TupleDescAttr(readState->tupleDescriptor, projectedColumnIndex);
    char *attr_name = NameStr(attr->attname);

    json column_metadata = readState->metadata["Columns"][attr_name];
    std::string attr_type = column_metadata["type"].get<std::string>();
    int start_offset = column_metadata["start_offset"].get<int>();
    int num_blocks = column_metadata["num_blocks"].get<int>();

    if (readState->blockIndex >= (uint32_t)num_blocks)
    {
      readState->blockSize = 0;
      readState->blockNext = 0;
      return;
    }

    int values_in_block = column_metadata["block_stats"][std::to_string(readState->blockIndex)]["num"].get<int>();

    readState->blockSize = values_in_block;
    readState->blockNext = 0;

    Datum *datumArray = readState->blockBuffers[projectedColumnIndex].values;

    if (attr_type == "str")
    {
      int block_offset = start_offset + readState->overallRow * STR_SIZE;
      StringInfo datumBuffer = ReadFromFile(readState->tableFile, block_offset, values_in_block * STR_SIZE);
      char *valueBuffer = readState->blockBuffers[projectedColumnIndex].valueBuffer;

      for (int i = 0; i < values_in_block; i++)
      {
        char *str = datumBuffer->data + i * STR_SIZE;
        int len = strlen(str);
        text *t = (text *)(valueBuffer + i * (STR_SIZE + VARHDRSZ));
        SET_VARSIZE(t, len + VARHDRSZ);
        memcpy(VARDATA(t), str, len);
        datumArray[i] = fetch_att((char *)t, false,
                                  STR_SIZE);
      }

      pfree(datumBuffer->data);
      pfree(datumBuffer);
    }
    else
    {

      int block_offset = start_offset + readState->overallRow * INT_FLOAT_SIZE;
      StringInfo raw_data = ReadFromFile(readState->tableFile, block_offset, values_in_block * INT_FLOAT_SIZE);
      for (int i = 0; i < values_in_block; i++)
      {
        datumArray[i] = fetch_att(raw_data->data + i * INT_FLOAT_SIZE, true, INT_FLOAT_SIZE);
      }
      pfree(raw_data->data);
      pfree(raw_data);
    }
  }
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *scanState)
{
  TableReadState *readState = (TableReadState *)scanState->fdw_state;
  TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
  bool nextRowFound = false;

  TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
  Datum *columnValues = tupleSlot->tts_values;
  bool *columnNulls = tupleSlot->tts_isnull;
  uint32 columnCount = tupleDescriptor->natts;

  /* initialize all values for this row to null */
  memset(columnValues, 0, columnCount * sizeof(Datum));
  memset(columnNulls, true, columnCount * sizeof(bool));

  ExecClearTuple(tupleSlot);

  if (readState->blockNext >= readState->blockSize)
  {
    fetch_more_data(scanState);

    if (readState->blockNext >= readState->blockSize)
    {
      return tupleSlot;
    }
  }

  nextRowFound = db721_ReadNextRow(readState, columnValues, columnNulls);

  if (nextRowFound)
  {
    ExecStoreVirtualTuple(tupleSlot);
  }

  return tupleSlot;
}

extern "C" void db721_EndForeignScan(ForeignScanState *node)
{
  TableReadState *readState = (TableReadState *)node->fdw_state;
  if (readState != NULL)
  {
    db721_EndRead(readState);
  }
}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node)
{
  db721_EndForeignScan(node);
  db721_BeginForeignScan(node, 0);
}
