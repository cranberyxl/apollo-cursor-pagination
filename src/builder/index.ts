// based on Relay's Connection spec at
// https://facebook.github.io/relay/graphql/connections.htm#sec-Pagination-algorithm

export interface OrderArgs<C, PK> {
  orderColumn: string | string[];
  ascOrDesc: 'asc' | 'desc' | ('asc' | 'desc')[];
  isAggregateFn?: (column: string) => boolean;
  formatColumnFn?: (column: string) => C;
  primaryKey: PK;
}

export interface GraphQLParams<PK = string> {
  before?: string;
  after?: string;
  first?: number;
  last?: number;
  orderDirection?: 'asc' | 'desc' | ('asc' | 'desc')[];
  orderBy?: string | string[] | PK;
}

export const encode = (str: string): string =>
  Buffer.from(str).toString('base64');
export const decode = (str: string): string =>
  Buffer.from(str, 'base64').toString();

export interface OperatorFunctions<N, NA, C, PK = string> {
  removeNodesBeforeAndIncluding: (
    nodeAccessor: NA,
    cursor: string,
    opts: OrderArgs<C, PK>
  ) => NA;
  removeNodesAfterAndIncluding: (
    nodeAccessor: NA,
    cursor: string,
    opts: OrderArgs<C, PK>
  ) => NA;
  removeNodesFromEnd: (
    nodeAccessor: NA,
    count: number,
    opts: OrderArgs<C, PK>
  ) => Promise<N[]>;
  removeNodesFromBeginning: (
    nodeAccessor: NA,
    count: number,
    opts: OrderArgs<C, PK>
  ) => Promise<N[]>;
  getNodesLength: (nodeAccessor: NA) => Promise<number>;
  hasLengthGreaterThan: (nodeAccessor: NA, count: number) => Promise<boolean>;
  convertNodesToEdges: (
    nodes: N[],
    params: GraphQLParams<PK> | undefined,
    opts: OrderArgs<C, PK>
  ) => { cursor: string; node: N }[];
  orderNodesBy: (nodeAccessor: NA, opts: OrderArgs<C, PK>) => NA;
}

export interface BuilderOptions<C = string, PK = string> {
  isAggregateFn?: (column: string) => boolean;
  formatColumnFn?: (column: string) => C;
  primaryKey: PK;
  skipTotalCount?: boolean;
  modifyEdgeFn?: <T>(edge: { cursor: string; node: T }) => {
    cursor: string;
    node: T;
  };
}

export interface PageInfo {
  hasPreviousPage: boolean;
  hasNextPage: boolean;
  startCursor?: string;
  endCursor?: string;
}

export interface ConnectionResult<T> {
  pageInfo: PageInfo;
  totalCount?: number;
  edges: { cursor: string; node: T }[];
}

/**
 * Slices the nodes list according to the `before` and `after` graphql query params.
 */
const applyCursorsToNodes = <N, NA, C, PK>(
  allNodesAccessor: NA,
  { before, after }: Pick<GraphQLParams<PK>, 'before' | 'after'>,
  {
    removeNodesBeforeAndIncluding,
    removeNodesAfterAndIncluding,
  }: Pick<
    OperatorFunctions<N, NA, C, PK>,
    'removeNodesBeforeAndIncluding' | 'removeNodesAfterAndIncluding'
  >,
  opts: OrderArgs<C, PK>
): NA => {
  let nodesAccessor = allNodesAccessor;
  if (after) {
    nodesAccessor = removeNodesBeforeAndIncluding(nodesAccessor, after, opts);
  }
  if (before) {
    nodesAccessor = removeNodesAfterAndIncluding(nodesAccessor, before, opts);
  }
  return nodesAccessor;
};

/**
 * Slices a node list according to `before`, `after`, `first` and `last` graphql query params.
 */
const nodesToReturn = async <N, NA, C = string, PK = string>(
  allNodesAccessor: NA,
  operatorFunctions: Pick<
    OperatorFunctions<N, NA, C, PK>,
    | 'removeNodesBeforeAndIncluding'
    | 'removeNodesAfterAndIncluding'
    | 'removeNodesFromEnd'
    | 'removeNodesFromBeginning'
    | 'orderNodesBy'
  >,
  {
    before,
    after,
    first,
    last,
  }: Pick<GraphQLParams<PK>, 'before' | 'after' | 'first' | 'last'>,
  opts: OrderArgs<C, PK>
): Promise<{ nodes: N[]; hasNextPage: boolean; hasPreviousPage: boolean }> => {
  const orderedNodesAccessor = operatorFunctions.orderNodesBy(
    allNodesAccessor,
    opts
  );
  const nodesAccessor = applyCursorsToNodes(
    orderedNodesAccessor,
    { before, after },
    {
      removeNodesBeforeAndIncluding:
        operatorFunctions.removeNodesBeforeAndIncluding,
      removeNodesAfterAndIncluding:
        operatorFunctions.removeNodesAfterAndIncluding,
    },
    opts
  );
  let hasNextPage = !!before;
  let hasPreviousPage = !!after;
  let nodes: N[] = [];
  if (first) {
    if (first < 0) throw new Error('`first` argument must not be less than 0');
    nodes = await operatorFunctions.removeNodesFromEnd(
      nodesAccessor,
      first + 1,
      opts
    );
    if (nodes.length > first) {
      hasNextPage = true;
      nodes = nodes.slice(0, first);
    }
  }
  if (last) {
    if (last < 0) throw new Error('`last` argument must not be less than 0');
    nodes = await operatorFunctions.removeNodesFromBeginning(
      nodesAccessor,
      last + 1,
      opts
    );
    if (nodes.length > last) {
      hasPreviousPage = true;
      nodes = nodes.slice(1);
    }
  }
  return { nodes, hasNextPage, hasPreviousPage };
};

/**
 * Returns a function that must be called to generate a Relay's Connection based page.
 */
const apolloCursorPaginationBuilder =
  <N, NA, C, PK = string>({
    removeNodesBeforeAndIncluding,
    removeNodesAfterAndIncluding,
    getNodesLength,
    removeNodesFromEnd,
    removeNodesFromBeginning,
    convertNodesToEdges,
    orderNodesBy,
  }: OperatorFunctions<N, NA, C, PK>) =>
  async (
    allNodesAccessor: NA,
    args: GraphQLParams<PK>,
    opts: BuilderOptions<C, PK>
  ): Promise<ConnectionResult<N>> => {
    const {
      isAggregateFn,
      formatColumnFn,
      skipTotalCount = false,
      modifyEdgeFn,
      primaryKey,
    } = opts;

    const {
      before,
      after,
      first,
      last,
      orderDirection = 'asc',
      orderBy = primaryKey,
    } = args;

    // Ensure we have valid orderColumn and primaryKey
    if (!primaryKey && !orderBy) {
      throw new Error('orderBy is required when primaryKey is not provided');
    }

    const orderColumn = orderBy as string | string[];
    const ascOrDesc = orderDirection;

    const { nodes, hasPreviousPage, hasNextPage } = await nodesToReturn(
      allNodesAccessor,
      {
        removeNodesBeforeAndIncluding,
        removeNodesAfterAndIncluding,
        removeNodesFromEnd,
        removeNodesFromBeginning,
        orderNodesBy,
      },
      {
        before,
        after,
        first,
        last,
      },
      {
        orderColumn,
        ascOrDesc,
        isAggregateFn,
        formatColumnFn,
        primaryKey,
      }
    );

    const totalCount = !skipTotalCount
      ? await getNodesLength(allNodesAccessor)
      : undefined;

    let edges = convertNodesToEdges(
      nodes,
      {
        before,
        after,
        first,
        last,
      },
      {
        orderColumn,
        ascOrDesc,
        isAggregateFn,
        formatColumnFn,
        primaryKey,
      }
    );
    if (modifyEdgeFn) {
      edges = edges.map((edge) => modifyEdgeFn(edge));
    }

    const startCursor = edges[0]?.cursor;
    const endCursor = edges[edges.length - 1]?.cursor;

    return {
      pageInfo: {
        hasPreviousPage,
        hasNextPage,
        startCursor,
        endCursor,
      },
      totalCount,
      edges,
    };
  };

export default apolloCursorPaginationBuilder;
