// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "nlohmann/json.hpp"
#include <locale>

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

#define TYPE_INT ((int)1)
#define TYPE_FLOAT ((int)2)
#define TYPE_STR ((int)3)

/* 
int
= 
> 
>=
< 
<=
<>
*/
#define OP_INT_EQ_LR 96
#define OP_INT_GT_LR 521
#define OP_INT_GTE_LR 525
#define OP_INT_LT_LR 97
#define OP_INT_LTE_LR 523
#define OP_INT_NEQ_LR 518

/*
float
=  
>  
>= 
<  
<= 
<> 
*/
#define OP_FLOAT_EQ_L 1120
#define OP_FLOAT_GT_L 1123
#define OP_FLOAT_GTE_L 1125
#define OP_FLOAT_LT_L 1122
#define OP_FLOAT_LTE_L 1124
#define OP_FLOAT_NEQ_L 1121

#define OP_FLOAT_EQ_R 1130
#define OP_FLOAT_GT_R 1133
#define OP_FLOAT_GTE_R 1135
#define OP_FLOAT_LT_R 1132
#define OP_FLOAT_LTE_R 1134
#define OP_FLOAT_NEQ_R 1131

/*
str
=  
>  
>= 
<  
<= 
<> 
*/
#define OP_STR_EQ_LR 98
#define OP_STR_GT_LR 666
#define OP_STR_GTE_LR 667
#define OP_STR_LT_LR 664
#define OP_STR_LTE_LR 665
#define OP_STR_NEQ_LR 531


#define METADATA_SIZE_OFFSET 4
#define STR_SIZE 32
#define INT_FLOAT_SIZE 4

// clang-format on
using json = nlohmann::json;

std::locale loc("en_US.UTF-8");

typedef struct BlockBuffer
{
  Datum *values;
  char *valueBuffer;
  int type;
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

  uint32_t numBlocks;

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

  if (stat(filename, &st) < 0)
  {
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("could not stat file \"%s\": %m", filename)));
  }

  metadataSizeBytes = ReadFromFile(tableFile, st.st_size - METADATA_SIZE_OFFSET, METADATA_SIZE_OFFSET);
  metadataSize = *((uint32_t *)metadataSizeBytes->data);

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

  readstate->numBlocks = 0;

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

    readstate->numBlocks = column_metadata["num_blocks"].get<int>();
    readstate->blockBuffers[projectedColumnIndex].values = (Datum *)palloc(max_block_size * sizeof(Datum));

    if (attr_type == "str")
    {
      readstate->blockBuffers[projectedColumnIndex].valueBuffer = (char *)palloc(max_block_size * (STR_SIZE + VARHDRSZ));
      readstate->blockBuffers[projectedColumnIndex].type = TYPE_STR;
    }
    else
    {
      if (attr_type == "int")
      {
        readstate->blockBuffers[projectedColumnIndex].type = TYPE_INT;
      }
      else if (attr_type == "float")
      {
        readstate->blockBuffers[projectedColumnIndex].type = TYPE_FLOAT;
      }
      else
      {
        ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                        errmsg("unsupported type %s", attr_type.c_str())));
      }

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
  List *actual_scan_clauses = NIL;
  List *filtered_scan_clauses = NIL;
  List *fdw_scan_clauses = NIL;

  /*
   * Although we skip row blocks that are refuted by the WHERE clause, but
   * we have no native ability to evaluate restriction clauses and make sure
   * that all non-related rows are filtered out. So we just put all of the
   * scanClauses into the plan node's qual list for the executor to check.
   */
  actual_scan_clauses = extract_actual_clauses(scan_clauses,
                                               false); /* extract regular clauses */
  ListCell *lc;
  foreach (lc, actual_scan_clauses)
  {
    Expr *clause = (Expr *)lfirst(lc);
    OpExpr *expr;
    Expr *left, *right;
    if (IsA(clause, OpExpr))
    {
      expr = (OpExpr *)clause;
      if (list_length(expr->args) == 2)
      {
        left = (Expr *)linitial(expr->args);
        right = (Expr *)lsecond(expr->args);
        /*
         * Looking for expressions like "EXPR OP CONST" or "CONST OP EXPR"
         */

        if (IsA(left, RelabelType))
        {
          left = (((RelabelType *)left)->arg);
        }
        if (IsA(right, RelabelType))
        {
          right = (((RelabelType *)right)->arg);
        }

        if ((IsA(right, Const) && IsA(left, Var)) || (IsA(left, Const) && IsA(right, Var)))
        {
          fdw_scan_clauses = lappend(fdw_scan_clauses, clause);
          continue;
        }
      }
    }

    filtered_scan_clauses = lappend(fdw_scan_clauses, clause);
  }

  /*
   * As an optimization, we only read columns that are present in the query.
   * To find these columns, we need baserel. We don't have access to baserel
   * in executor's callback functions, so we get the column list here and put
   * it into foreign scan node's private list.
   */
  columnList = ColumnList(baserel, foreigntableid);
  foreignPrivateList = list_make1(columnList);

  /* create the foreign scan node */
  foreignScan = make_foreignscan(tlist, filtered_scan_clauses, baserel->relid,
                                 NIL, /* no expressions to evaluate */
                                 foreignPrivateList,
                                 NIL,
                                 fdw_scan_clauses,
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
  whereClauseList = foreignScan->fdw_recheck_quals;

  columnList = (List *)linitial(foreignPrivateList);

  readState = db721_BeginRead(GetFileName(foreignTableId), tupleDescriptor,
                              columnList, whereClauseList);

  scanState->fdw_state = (void *)readState;
}

bool evaluate_op(Oid opno, Datum left, Datum right, int type)
{
  if (type == TYPE_INT)
  {
    int left_int = DatumGetInt32(left);
    int right_int = DatumGetInt32(right);
    switch (opno)
    {
    case OP_INT_LT_LR: // <
      return left_int < right_int;
    case OP_INT_LTE_LR: // <=
      return left_int <= right_int;
    case OP_INT_EQ_LR: // =
      return left_int == right_int;
    case OP_INT_GTE_LR: // >=
      return left_int >= right_int;
    case OP_INT_GT_LR: // >
      return left_int > right_int;
    case OP_INT_NEQ_LR: // <>
      return left_int != right_int;
    default:
      elog(ERROR, "unknown opno %u", opno);
    }
  }
  else if (type == TYPE_FLOAT)
  {
    double left_float;
    double right_float;

    switch (opno)
    {
    case OP_FLOAT_LT_L:  // <
    case OP_FLOAT_LTE_L: // <=
    case OP_FLOAT_EQ_L:  // =
    case OP_FLOAT_GTE_L: // >=
    case OP_FLOAT_GT_L:  // >
    case OP_FLOAT_NEQ_L: // <>
      left_float = static_cast<double>(DatumGetFloat4(left));
      right_float = DatumGetFloat8(right);
      break;
    case OP_FLOAT_LT_R:  // <
    case OP_FLOAT_LTE_R: // <=
    case OP_FLOAT_EQ_R:  // =
    case OP_FLOAT_GTE_R: // >=
    case OP_FLOAT_GT_R:  // >
    case OP_FLOAT_NEQ_R: // <>
      left_float = DatumGetFloat8(left);
      right_float = static_cast<double>(DatumGetFloat4(right));
      break;
    default:
      elog(ERROR, "unknown opno %u", opno);
    }

    switch (opno)
    {
    case OP_FLOAT_LT_L: // <
    case OP_FLOAT_LT_R: // <
      return left_float < right_float;
    case OP_FLOAT_LTE_L: // <=
    case OP_FLOAT_LTE_R: // <=
      return left_float <= right_float;
    case OP_FLOAT_EQ_L: // =
    case OP_FLOAT_EQ_R: // =
      return left_float == right_float;
    case OP_FLOAT_GTE_L: // >=
    case OP_FLOAT_GTE_R: // >=
      return left_float >= right_float;
    case OP_FLOAT_GT_L: // >
    case OP_FLOAT_GT_R: // >
      return left_float > right_float;
    case OP_FLOAT_NEQ_L: // <>
    case OP_FLOAT_NEQ_R: // <>
      return left_float != right_float;
    default:
      elog(ERROR, "unknown opno %u", opno);
    }
  }
  else if (type == TYPE_STR)
  {
    char *tl = DatumGetPointer(left);
    char *tr = DatumGetPointer(right);
    std::string left_text(VARDATA(tl), VARSIZE(tl) - VARHDRSZ);
    std::string right_text(VARDATA(tr), VARSIZE(tr) - VARHDRSZ);
    switch (opno)
    {
    case OP_STR_LT_LR: // <
      return loc(left_text, right_text);
    case OP_STR_LTE_LR: // <=
      return left_text == right_text || loc(left_text, right_text);
    case OP_STR_EQ_LR: // =
      return left_text == right_text;
    case OP_STR_GTE_LR: // >=
      return left_text == right_text || loc(right_text, left_text);
    case OP_STR_GT_LR: // >
      return loc(right_text, left_text);
    case OP_STR_NEQ_LR: // <>
      return left_text != right_text;
    default:
      elog(ERROR, "unknown opno %u", opno);
    }
  }
  else
  {
    elog(ERROR, "unknown type %d", type);
  }
}

bool evaluate_where_clause(TableReadState *readState, Datum *columnValues, bool *columnNulls)
{
  List *whereClauseList = readState->whereClauseList;
  ListCell *whereClauseCell = NULL;

  foreach (whereClauseCell, whereClauseList)
  {
    Expr *whereClause = (Expr *)lfirst(whereClauseCell);

    assert(IsA(whereClause, OpExpr));

    OpExpr *expr = (OpExpr *)whereClause;
    Expr *left = (Expr *)linitial(expr->args);
    Expr *right = (Expr *)lsecond(expr->args);

    if (IsA(left, RelabelType))
    {
      left = (((RelabelType *)left)->arg);
    }
    if (IsA(right, RelabelType))
    {
      right = (((RelabelType *)right)->arg);
    }

    Datum left_datum;
    Datum right_datum;
    uint32_t columnIndex;
    int type;

    if (IsA(left, Const) && IsA(right, Var))
    {
      left_datum = ((Const *)left)->constvalue;

      columnIndex = ((Var *)right)->varattno - 1;
      right_datum = columnValues[columnIndex];
    }
    else if (IsA(left, Var) && IsA(right, Const))
    {
      columnIndex = ((Var *)left)->varattno - 1;
      left_datum = columnValues[columnIndex];

      right_datum = ((Const *)right)->constvalue;
    }
    else
    {
      elog(ERROR, "unknown case");
    }

    if (columnNulls[columnIndex])
    {
      return false;
    }

    // get type
    type = readState->blockBuffers[columnIndex].type;

    if (!evaluate_op(expr->opno, left_datum, right_datum, type))
    {
      return false;
    }
  }
  return true;
}

bool db721_ReadNextRow(TableReadState *readState, Datum *columnValues, bool *columnNulls)
{
  while (readState->blockNext < readState->blockSize)
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

    if (evaluate_where_clause(readState, columnValues, columnNulls))
    {
      return true;
    }
  }

  return false;
}

void build_cstring(char *src_str, char *dest_buffer)
{
  int len = strlen(src_str);
  text *t = (text *)dest_buffer;
  SET_VARSIZE(t, len + VARHDRSZ);
  memcpy(VARDATA(t), src_str, len);
}

bool should_read_block(TableReadState *readState, uint32_t blockIndex)
{
  List *whereClauseList = readState->whereClauseList;
  ListCell *whereClauseCell = NULL;

  foreach (whereClauseCell, whereClauseList)
  {
    Expr *whereClause = (Expr *)lfirst(whereClauseCell);

    assert(IsA(whereClause, OpExpr));

    OpExpr *expr = (OpExpr *)whereClause;
    Expr *left = (Expr *)linitial(expr->args);
    Expr *right = (Expr *)lsecond(expr->args);
    Oid opno = expr->opno;

    if (opno == OP_INT_NEQ_LR || opno == OP_STR_NEQ_LR || opno == OP_FLOAT_NEQ_L || opno == OP_FLOAT_NEQ_R)
    {
      continue;
    }

    if (IsA(left, RelabelType))
    {
      left = (((RelabelType *)left)->arg);
    }
    if (IsA(right, RelabelType))
    {
      right = (((RelabelType *)right)->arg);
    }

    Datum const_datum;
    int type;
    uint32_t columnIndex;
    if (IsA(left, Const) && IsA(right, Var))
    {
      const_datum = ((Const *)left)->constvalue;
      columnIndex = ((Var *)right)->varattno - 1;
    }
    else if (IsA(left, Var) && IsA(right, Const))
    {
      const_datum = ((Const *)right)->constvalue;
      columnIndex = ((Var *)left)->varattno - 1;
    }
    else
    {
      elog(ERROR, "unknown case");
    }

    // get type
    type = readState->blockBuffers[columnIndex].type;

    Form_pg_attribute attr = TupleDescAttr(readState->tupleDescriptor, columnIndex);
    char *attr_name = NameStr(attr->attname);

    json block_metadata = readState->metadata["Columns"][attr_name]["block_stats"][std::to_string(blockIndex)];
    json min_val = block_metadata["min"];
    json max_val = block_metadata["max"];

    char min_buffer[STR_SIZE + VARHDRSZ];
    char max_buffer[STR_SIZE + VARHDRSZ];
    Datum min_datum;
    Datum max_datum;

    switch (type)
    {
    case TYPE_FLOAT:
    {
      float min_float = static_cast<float>(min_val.get<double>());
      float max_float = static_cast<float>(max_val.get<double>());
      min_datum = fetch_att(&min_float, true, INT_FLOAT_SIZE);
      max_datum = fetch_att(&max_float, true, INT_FLOAT_SIZE);
      break;
    }
    case TYPE_INT:
    {
      int min_int = min_val.get<int>();
      int max_int = max_val.get<int>();
      min_datum = fetch_att(&min_int, true, INT_FLOAT_SIZE);
      max_datum = fetch_att(&max_int, true, INT_FLOAT_SIZE);
      break;
    }
    case TYPE_STR:
    {
      std::string min_str = min_val.get<std::string>();
      std::string max_str = max_val.get<std::string>();

      build_cstring((char *)min_str.c_str(), min_buffer);
      build_cstring((char *)max_str.c_str(), max_buffer);

      min_datum = PointerGetDatum(min_buffer);
      max_datum = PointerGetDatum(max_buffer);
      break;
    }
    default:
      elog(ERROR, "unknown type");
    }

    if (opno == OP_INT_EQ_LR || opno == OP_STR_EQ_LR || opno == OP_FLOAT_EQ_L || opno == OP_FLOAT_EQ_R)
    {
      Oid op_le;
      Oid op_ge;

      switch (opno)
      {
      case OP_INT_EQ_LR:
        op_le = OP_INT_LTE_LR;
        op_ge = OP_INT_GTE_LR;
        break;
      case OP_STR_EQ_LR:
        op_le = OP_STR_LTE_LR;
        op_ge = OP_STR_GTE_LR;
        break;
      case OP_FLOAT_EQ_L:
      case OP_FLOAT_EQ_R:
        op_le = OP_FLOAT_LTE_L;
        op_ge = OP_FLOAT_GTE_L;
        break;
      default:
        elog(ERROR, "unknown case");
      }

      if (!(evaluate_op(op_le, min_datum, const_datum, type) && evaluate_op(op_ge, max_datum, const_datum, type)))
      {
        return false;
      }
      continue;
    }

    Datum left_datum;
    Datum right_datum;

    if (IsA(left, Const) && IsA(right, Var))
    {
      left_datum = ((Const *)left)->constvalue;
      switch (opno)
      {
      case OP_INT_LT_LR:
      case OP_INT_LTE_LR:
      case OP_STR_LT_LR:
      case OP_STR_LTE_LR:
      case OP_FLOAT_LT_R:
      case OP_FLOAT_LTE_R:
        right_datum = max_datum;
        break;
      case OP_INT_GT_LR:
      case OP_INT_GTE_LR:
      case OP_STR_GT_LR:
      case OP_STR_GTE_LR:
      case OP_FLOAT_GT_R:
      case OP_FLOAT_GTE_R:
        right_datum = min_datum;
        break;
      default:
        elog(ERROR, "unknown case");
      }
    }
    else if (IsA(left, Var) && IsA(right, Const))
    {
      right_datum = ((Const *)right)->constvalue;
      switch (opno)
      {
      case OP_INT_LT_LR:
      case OP_INT_LTE_LR:
      case OP_STR_LT_LR:
      case OP_STR_LTE_LR:
      case OP_FLOAT_LT_L:
      case OP_FLOAT_LTE_L:
        left_datum = min_datum;
        break;
      case OP_INT_GT_LR:
      case OP_INT_GTE_LR:
      case OP_STR_GT_LR:
      case OP_STR_GTE_LR:
      case OP_FLOAT_GT_L:
      case OP_FLOAT_GTE_L:
        left_datum = max_datum;
        break;
      default:
        elog(ERROR, "unknown case");
      }
    }
    else
    {
      elog(ERROR, "unknown case");
    }

    if (!evaluate_op(opno, left_datum, right_datum, type))
    {
      return false;
    }
  }

  return true;
}

void fetch_more_data(ForeignScanState *scanState)
{
  TableReadState *readState = (TableReadState *)scanState->fdw_state;
  assert(readState->blockNext >= readState->blockSize);
  ListCell *projectedColumnCell = NULL;

  json some_col_block_stats = (readState->metadata["Columns"].begin()).value()["block_stats"];
  int values_in_block;

  bool block_pass = false;
  do
  {
    readState->blockIndex++;
    // make sure there is a still a block to read
    if (readState->blockIndex >= readState->numBlocks)
    {
      readState->blockSize = 0;
      readState->blockNext = 0;
      return;
    }

    values_in_block = some_col_block_stats[std::to_string(readState->blockIndex)]["num"].get<int>();

    // check all conditions on block
    block_pass = should_read_block(readState, readState->blockIndex);
    if (!block_pass)
    {
      readState->overallRow += values_in_block;
    }
  } while (!block_pass);

  readState->blockSize = values_in_block;
  readState->blockNext = 0;

  foreach (projectedColumnCell, readState->projectedColumnList)
  {

    Var *projectedColumn = (Var *)lfirst(projectedColumnCell);
    uint32 projectedColumnIndex = projectedColumn->varattno - 1;
    Form_pg_attribute attr = TupleDescAttr(readState->tupleDescriptor, projectedColumnIndex);
    char *attr_name = NameStr(attr->attname);

    json column_metadata = readState->metadata["Columns"][attr_name];
    std::string attr_type = column_metadata["type"].get<std::string>();
    int start_offset = column_metadata["start_offset"].get<int>();

    Datum *datumArray = readState->blockBuffers[projectedColumnIndex].values;

    if (attr_type == "str")
    {
      int block_offset = start_offset + readState->overallRow * STR_SIZE;
      StringInfo datumBuffer = ReadFromFile(readState->tableFile, block_offset, values_in_block * STR_SIZE);
      char *valueBuffer = readState->blockBuffers[projectedColumnIndex].valueBuffer;

      for (int i = 0; i < values_in_block; i++)
      {
        char *str = datumBuffer->data + i * STR_SIZE;
        char *dest_buffer = valueBuffer + i * (STR_SIZE + VARHDRSZ);
        build_cstring(str, dest_buffer);
        datumArray[i] = PointerGetDatum(dest_buffer);
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

  do
  {
    if (readState->blockNext >= readState->blockSize)
    {
      fetch_more_data(scanState);

      if (readState->blockNext >= readState->blockSize)
      {
        return tupleSlot;
      }
    }
    nextRowFound = db721_ReadNextRow(readState, columnValues, columnNulls);
  } while (!nextRowFound);

  ExecStoreVirtualTuple(tupleSlot);

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
