import type {
  Entity,
  FormattedItem,
  QueryCommand,
  Table,
  Query,
  QueryOptions,
  KeyInputItem,
} from 'dynamodb-toolbox';
import apolloCursorPaginationBuilder, { encode, decode } from '../../builder';

const SEPARATION_TOKEN = '_*_';
const ARRAY_DATA_SEPARATION_TOKEN = '_%_';

interface DynamoDBOperatorFunctions<
  TABLE extends Table = Table,
  ENTITY extends Entity = Entity,
  QUERY extends Query<TABLE> = Query<TABLE>,
  OPTIONS extends QueryOptions<TABLE, ENTITY[], QUERY> = QueryOptions<
    TABLE,
    ENTITY[],
    QUERY
  >,
> {
  removeNodesBeforeAndIncluding: (
    nodeAccessor: QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>,
    cursor: string,
    opts: {
      orderColumn: string | string[];
      ascOrDesc: 'asc' | 'desc' | ('asc' | 'desc')[];
      isAggregateFn?: (column: string) => boolean;
      formatColumnFn?: (column: string) => never;
      primaryKey: KeyInputItem<ENTITY>;
    }
  ) => QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>;
  removeNodesAfterAndIncluding: (
    nodeAccessor: QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>,
    cursor: string,
    opts: {
      orderColumn: string | string[];
      ascOrDesc: 'asc' | 'desc' | ('asc' | 'desc')[];
      isAggregateFn?: (column: string) => boolean;
      formatColumnFn?: (column: string) => never;
      primaryKey: KeyInputItem<ENTITY>;
    }
  ) => QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>;
  removeNodesFromEnd: (
    nodeAccessor: QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>,
    count: number,
    opts: {
      orderColumn: string | string[];
      ascOrDesc: 'asc' | 'desc' | ('asc' | 'desc')[];
      isAggregateFn?: (column: string) => boolean;
      formatColumnFn?: (column: string) => never;
      primaryKey: KeyInputItem<ENTITY>;
    }
  ) => Promise<FormattedItem<ENTITY>[]>;
  removeNodesFromBeginning: (
    nodeAccessor: QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>,
    count: number,
    opts: {
      orderColumn: string | string[];
      ascOrDesc: 'asc' | 'desc' | ('asc' | 'desc')[];
      isAggregateFn?: (column: string) => boolean;
      formatColumnFn?: (column: string) => never;
      primaryKey: KeyInputItem<ENTITY>;
    }
  ) => Promise<FormattedItem<ENTITY>[]>;
  getNodesLength: (
    nodeAccessor: QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>
  ) => Promise<number>;
  hasLengthGreaterThan: (
    nodeAccessor: QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>,
    count: number
  ) => Promise<boolean>;
  convertNodesToEdges: (
    nodes: FormattedItem<ENTITY>[],
    params: any,
    opts: {
      orderColumn: string | string[];
      ascOrDesc: 'asc' | 'desc' | ('asc' | 'desc')[];
      isAggregateFn?: (column: string) => boolean;
      formatColumnFn?: (column: string) => never;
      primaryKey: KeyInputItem<ENTITY>;
    }
  ) => { cursor: string; node: FormattedItem<ENTITY> }[];
  orderNodesBy: (
    nodeAccessor: QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>,
    opts: {
      orderColumn: string | string[];
      ascOrDesc: 'asc' | 'desc' | ('asc' | 'desc')[];
      isAggregateFn?: (column: string) => boolean;
      formatColumnFn?: (column: string) => never;
      primaryKey: KeyInputItem<ENTITY>;
    }
  ) => QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>;
}

const cursorGenerator = (
  id: string | number,
  customColumnValue: string
): string => encode(`${id}${SEPARATION_TOKEN}${customColumnValue}`);

const getDataFromCursor = (cursor: string): [string, any[]] => {
  const decodedCursor = decode(cursor);
  const data = decodedCursor.split(SEPARATION_TOKEN);
  if (data[0] === undefined || data[1] === undefined) {
    throw new Error(`Could not find edge with cursor ${cursor}`);
  }
  const values = data[1]
    .split(ARRAY_DATA_SEPARATION_TOKEN)
    .map((v) => JSON.parse(v));
  return [data[0], values];
};

const operateOverScalarOrArray = <R>(
  initialValue: R,
  scalarOrArray: string | string[],
  operation: (scalar: string, index: number | null, prev: R) => R,
  operateResult?: (result: R, isArray: boolean) => R
): R => {
  let result = initialValue;
  const isArray = Array.isArray(scalarOrArray);
  if (isArray) {
    scalarOrArray.forEach((scalar, index) => {
      result = operation(scalar, index, result);
    });
  } else {
    result = operation(scalarOrArray, null, result);
  }
  if (operateResult) {
    result = operateResult(result, isArray);
  }
  return result;
};

export default function paginate<
  TABLE extends Table = Table,
  ENTITY extends Entity = Entity,
  QUERY extends Query<TABLE> = Query<TABLE>,
  OPTIONS extends QueryOptions<TABLE, ENTITY[], QUERY> = QueryOptions<
    TABLE,
    ENTITY[],
    QUERY
  >,
>() {
  return apolloCursorPaginationBuilder<
    FormattedItem<ENTITY>,
    QueryCommand<TABLE, ENTITY[], QUERY, OPTIONS>,
    never,
    KeyInputItem<ENTITY>
  >({
    removeNodesBeforeAndIncluding: (nodeAccessor, cursor, opts) => nodeAccessor,
    removeNodesAfterAndIncluding: (nodeAccessor, cursor, opts) => nodeAccessor,
    removeNodesFromEnd: async (nodeAccessor, count, opts) => {
      // Use DynamoDB's limit functionality
      const result = await nodeAccessor.send();
      const items = result.Items || [];
      return items.slice(0, count);
    },
    removeNodesFromBeginning: async (nodeAccessor, count, opts) => {
      // For reverse queries, we need to invert the query direction
      // This is a simplified implementation
      const result = await nodeAccessor.send();
      const items = result.Items || [];
      // Get the last N items and reverse them back
      return items.slice(-count).reverse();
    },
    getNodesLength: async (nodeAccessor) => {
      const result = await nodeAccessor.send();
      return result.Count || 0;
    },
    hasLengthGreaterThan: async (nodeAccessor, count) => {
      const result = await nodeAccessor.send();
      const items = result.Items || [];
      return items.length > count;
    },
    convertNodesToEdges: (nodes, params, opts) => {
      const { orderColumn, primaryKey } = opts;

      return nodes.map((node) => {
        const dataValue = operateOverScalarOrArray(
          '',
          orderColumn,
          (orderBy, index, prev) => {
            const nodeValue = node[orderBy as keyof FormattedItem<ENTITY>];
            if (nodeValue === undefined) {
              return prev;
            }
            const result = `${prev}${index ? ARRAY_DATA_SEPARATION_TOKEN : ''}${JSON.stringify(nodeValue)}`;
            return result;
          }
        );

        // Extract primary key value
        const nodePrimaryKey = node[primaryKey as keyof FormattedItem<ENTITY>];
        if (nodePrimaryKey === undefined) {
          throw new Error(
            `Could not find primary key ${String(primaryKey)} in node`
          );
        }

        return {
          cursor: cursorGenerator(String(nodePrimaryKey), dataValue),
          node,
        };
      });
    },
    orderNodesBy: (nodeAccessor, opts) => nodeAccessor,
  });
}
