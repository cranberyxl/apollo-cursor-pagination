import { describe, expect, it } from '@jest/globals';
import apolloCursorPaginationBuilder, {
  OperatorFunctions,
  OrderArgs,
} from '..';

interface Node {
  id: number;
  name: string;
}

describe('apolloCursorPaginationBuilder', () => {
  const mockNodes: Node[] = [
    { id: 1, name: 'Node 1' },
    { id: 2, name: 'Node 2' },
    { id: 3, name: 'Node 3' },
    { id: 4, name: 'Node 4' },
    { id: 5, name: 'Node 5' },
  ];

  const mockOperatorFunctions: OperatorFunctions<Node, Node[], string> = {
    removeNodesBeforeAndIncluding: (nodes: Node[], cursor: string) => {
      const cursorId = parseInt(cursor.split(':')[1], 10);
      return nodes.filter((node) => node.id > cursorId);
    },
    removeNodesAfterAndIncluding: (nodes: Node[], cursor: string) => {
      const cursorId = parseInt(cursor.split(':')[1], 10);
      return nodes.filter((node) => node.id < cursorId);
    },
    getNodesLength: async (nodes: Node[]) => nodes.length,
    hasLengthGreaterThan: async (nodes: Node[], length: number) =>
      nodes.length > length,
    removeNodesFromEnd: async (nodes: Node[], first: number) =>
      nodes.slice(0, first),
    removeNodesFromBeginning: async (nodes: Node[], last: number) =>
      nodes.slice(-last),
    convertNodesToEdges: (nodes: Node[]) =>
      nodes.map((node) => ({
        cursor: `cursor:${node.id}`,
        node,
      })),
    orderNodesBy: (nodes: Node[], options: OrderArgs<string, string>) =>
      [...nodes].sort((a, b) => {
        const aVal = a[options.orderColumn as keyof Node];
        const bVal = b[options.orderColumn as keyof Node];
        return options.ascOrDesc === 'asc'
          ? Number(aVal) - Number(bVal)
          : Number(bVal) - Number(aVal);
      }),
  };

  const paginationBuilder = apolloCursorPaginationBuilder<Node, Node[], string>(
    mockOperatorFunctions
  );

  it('should return first N nodes when first is specified', async () => {
    const params = {
      first: 2,
    };
    const result = await paginationBuilder(mockNodes, params, {
      primaryKey: 'id',
    });
    expect(result.edges).toHaveLength(2);
    expect(result.edges[0].node).toEqual(mockNodes[0]);
    expect(result.edges[1].node).toEqual(mockNodes[1]);
    expect(result.pageInfo.hasNextPage).toBe(true);
    expect(result.pageInfo.hasPreviousPage).toBe(false);
  });

  it('should return last N nodes when last is specified', async () => {
    const params = {
      last: 2,
    };
    const result = await paginationBuilder(mockNodes, params, {
      primaryKey: 'id',
    });
    expect(result.edges).toHaveLength(2);
    expect(result.edges[0].node).toEqual(mockNodes[3]);
    expect(result.edges[1].node).toEqual(mockNodes[4]);
    expect(result.pageInfo.hasNextPage).toBe(false);
    expect(result.pageInfo.hasPreviousPage).toBe(true);
  });

  it('should handle after cursor correctly', async () => {
    const params = {
      first: 2,
      after: 'cursor:2',
    };
    const result = await paginationBuilder(mockNodes, params, {
      primaryKey: 'id',
    });
    expect(result.edges).toHaveLength(2);
    expect(result.edges[0].node).toEqual(mockNodes[2]);
    expect(result.edges[1].node).toEqual(mockNodes[3]);
  });

  it('should handle before cursor correctly', async () => {
    const params = {
      last: 2,
      before: 'cursor:4',
    };
    const result = await paginationBuilder(mockNodes, params, {
      primaryKey: 'id',
    });
    expect(result.edges).toHaveLength(2);
    expect(result.edges[0].node).toEqual(mockNodes[1]);
    expect(result.edges[1].node).toEqual(mockNodes[2]);
  });

  it('should throw error when first is negative', async () => {
    const params = {
      first: -1,
    };
    await expect(
      paginationBuilder(mockNodes, params, {
        primaryKey: 'id',
      })
    ).rejects.toThrow('`first` argument must not be less than 0');
  });

  it('should throw error when last is negative', async () => {
    const params = {
      last: -1,
    };
    await expect(
      paginationBuilder(mockNodes, params, {
        primaryKey: 'id',
      })
    ).rejects.toThrow('`last` argument must not be less than 0');
  });

  it('should handle custom ordering', async () => {
    const result = await paginationBuilder(
      mockNodes,
      {
        first: 3,
        orderBy: 'id',
        orderDirection: 'desc',
      },
      {
        primaryKey: 'id',
      }
    );
    expect(result.edges).toHaveLength(3);
    expect(result.edges[0].node).toEqual(mockNodes[4]);
    expect(result.edges[1].node).toEqual(mockNodes[3]);
    expect(result.edges[2].node).toEqual(mockNodes[2]);
  });
});
