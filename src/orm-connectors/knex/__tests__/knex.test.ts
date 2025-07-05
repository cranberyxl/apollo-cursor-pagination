import { describe, expect, it } from '@jest/globals';
import knex, { Knex } from 'knex';
import { Factory } from 'rosie';
import { faker } from '@faker-js/faker';
import paginate, {
  cursorGenerator,
  getDataFromCursor,
  orderNodesBy,
  convertNodesToEdges,
  getNodesLength,
  hasLengthGreaterThan,
  formatColumnIfAvailable,
  removeNodesBeforeAndIncluding,
  removeNodesFromEnd,
  removeNodesAfterAndIncluding,
  removeNodesFromBeginning,
} from '..';
import { decode, encode } from '../../../builder';

interface TestNode {
  id: number;
  name: string | null;
  age: number;
}

interface Edge<T> {
  cursor: string;
  node: T;
}

const factory = Factory.define<TestNode>('test_node').attrs({
  name: faker.lorem.word,
  age: faker.number.int,
  id: faker.number.int,
});

describe('non-db functions', () => {
  it('cursorGenerator', () => {
    const id = '123';
    const customColumnValue = '{"test": "test"}';
    const cursor = cursorGenerator(id, customColumnValue);
    expect(cursor).toBe('MTIzXypfeyJ0ZXN0IjogInRlc3QifQ==');
  });

  it('getDataFromCursor', () => {
    const cursor = 'MTIzXypfeyJ0ZXN0IjogInRlc3QifQ==';
    const [decodedId, decodedValues] = getDataFromCursor(cursor);
    expect(decodedId).toBe('123');
    expect(decodedValues).toEqual([{ test: 'test' }]);
  });

  it('getDataFromCursor throws on bad cursor', () => {
    const cursor = 'not-a-cursor';
    expect(() => getDataFromCursor(cursor)).toThrow(
      `Could not find edge with cursor ${cursor}`
    );
  });

  it('default encode and decode is base64', () => {
    expect(encode('test')).toBe('dGVzdA==');
    expect(decode('dGVzdA==')).toBe('test');
    expect(decode(encode('test'))).toBe('test');
  });
  it('formatColumnIfAvailable', () => {
    expect(formatColumnIfAvailable('test', undefined, true)).toBe('test');
    expect(
      formatColumnIfAvailable(
        'test',
        (column: string) => `${column} test`,
        true
      )
    ).toBe('test test');
    expect(
      formatColumnIfAvailable(
        'test',
        (column: string) => `${column} test`,
        false
      )
    ).toBe('test test');

    const mockFormatColumnFn = jest.fn().mockReturnValue('test');
    expect(formatColumnIfAvailable('test', mockFormatColumnFn)).toBe('test');
    expect(mockFormatColumnFn).toHaveBeenCalledWith('test', true);
  });

  it('convertNodesToEdges', () => {
    const nodes = [{ id: 1, name: 'test', age: 1 }];
    const result = convertNodesToEdges(nodes, undefined, {
      orderColumn: 'id',
      primaryKey: 'id',
      ascOrDesc: 'asc',
    });
    expect(result).toEqual([
      { cursor: 'MV8qXzE=', node: { id: 1, name: 'test', age: 1 } },
    ]);
  });
});

describe('Knex Custom Pagination with SQLite', () => {
  let db: Knex;

  beforeAll(() => {
    db = knex({
      client: 'sqlite3',
      connection: ':memory:',
      useNullAsDefault: true,
    });
  });

  afterAll(async () => {
    await db.destroy();
  });

  beforeEach(async () => {
    await db.schema.createTable('test_table', (table) => {
      table.increments('id').primary();
      table.string('name');
      table.integer('age');
    });
  });

  afterEach(async () => {
    await db.schema.dropTable('test_table');
  });

  describe('getNodesLength', () => {
    it('returns the number of nodes in a query', async () => {
      await db('test_table').insert(factory.build());
      const result = await getNodesLength(db('test_table'));
      expect(result).toBe(1);
    });
    it('returns the number of nodes a query with a select', async () => {
      await db('test_table').insert(factory.build());
      const result = await getNodesLength(db('test_table').select('age'));
      expect(result).toBe(1);
    });
  });

  describe('hasLengthGreaterThan', () => {
    it('returns true if the query has more than the given amount of nodes', async () => {
      await db('test_table').insert(factory.build());
      const result = await hasLengthGreaterThan(db('test_table'), 0);
      expect(result).toBe(true);
    });

    it('returns false if the query has less than the given amount of nodes', async () => {
      await db('test_table').insert(factory.build());
      const result = await hasLengthGreaterThan(db('test_table'), 1);
      expect(result).toBe(false);
    });
  });

  describe('orderNodesBy', () => {
    it('defaults', async () => {
      const nodes = factory.buildList(10);
      await db('test_table').insert(nodes);
      const result = await orderNodesBy(db('test_table'), {
        orderColumn: 'id',
        ascOrDesc: 'asc',
        primaryKey: 'id',
        formatColumnFn: undefined,
      });
      expect(result).toEqual(nodes.sort((a, b) => a.id - b.id));
    });
    it('formats column', async () => {
      const nodes = factory.buildList(10);
      await db('test_table').insert(nodes);
      const result = await orderNodesBy(db('test_table'), {
        orderColumn: 'age',
        ascOrDesc: 'asc',
        primaryKey: 'id',
        formatColumnFn: (c: string) => {
          if (c === 'age') return 'id';
          return c;
        },
      });
      expect(result).toEqual(nodes.sort((a, b) => a.id - b.id));
    });
    it('orders the nodes by the given column', async () => {
      const nodes = factory.buildList(10);
      await db('test_table').insert(nodes);
      const result = await orderNodesBy(db('test_table'), {
        orderColumn: 'age',
        ascOrDesc: 'asc',
        primaryKey: 'id',
        formatColumnFn: undefined,
      });
      expect(result).toEqual(nodes.sort((a, b) => a.age - b.age));
    });
    it('orders the nodes by the given column desc', async () => {
      const nodes = factory.buildList(10);
      await db('test_table').insert(nodes);
      const result = await orderNodesBy(db('test_table'), {
        orderColumn: 'age',
        ascOrDesc: 'desc',
        primaryKey: 'id',
        formatColumnFn: undefined,
      });
      expect(result).toEqual(nodes.sort((a, b) => b.age - a.age));
    });
  });
  describe('removeNodesBeforeAndIncluding', () => {
    // Used when `after` is included in the query
    // It must slice the result set from the element after the one with the given cursor until the end.
    // e.g. let [A, B, C, D] be the `resultSet`
    // removeNodesBeforeAndIncluding(resultSet, 'B') should return [C, D]
    it('id asc', async () => {
      const nodes = factory.buildList(10).sort((a, b) => a.id - b.id);
      const edges = convertNodesToEdges(nodes, undefined, {
        orderColumn: 'id',
        primaryKey: 'id',
        ascOrDesc: 'asc',
      });
      await db('test_table').insert(nodes);
      const result = await removeNodesBeforeAndIncluding(
        db('test_table'),
        edges[5].cursor,
        {
          orderColumn: 'id',
          primaryKey: 'id',
          ascOrDesc: 'asc',
          isAggregateFn: undefined,
          formatColumnFn: undefined,
        }
      );
      expect(result).toEqual(nodes.slice(6));
    });
    it('id desc', async () => {
      const nodes = factory.buildList(10).sort((a, b) => a.id - b.id);
      const edges = convertNodesToEdges(nodes, undefined, {
        orderColumn: 'id',
        primaryKey: 'id',
        ascOrDesc: 'desc',
      });
      await db('test_table').insert(nodes);
      const result = await removeNodesBeforeAndIncluding(
        db('test_table'),
        edges[5].cursor,
        {
          orderColumn: 'id',
          primaryKey: 'id',
          ascOrDesc: 'desc',
          isAggregateFn: undefined,
          formatColumnFn: undefined,
        }
      );

      expect(result).toEqual(nodes.slice(0, 5));
    });
  });

  describe('removeNodesAfterAndIncluding', () => {
    // Used when `before` is included in the query
    // It must remove all nodes after and including the one with cursor `cursorOfInitialNode`
    // e.g. let [A, B, C, D] be the `resultSet`
    // removeNodesAfterAndIncluding(resultSet, 'C') should return [A, B]

    it('id asc', async () => {
      const nodes = factory.buildList(10).sort((a, b) => a.id - b.id);
      const edges = convertNodesToEdges(nodes, undefined, {
        orderColumn: 'id',
        primaryKey: 'id',
        ascOrDesc: 'asc',
      });
      await db('test_table').insert(nodes);
      const result = await removeNodesAfterAndIncluding(
        db('test_table'),
        edges[5].cursor,
        {
          orderColumn: 'id',
          primaryKey: 'id',
          ascOrDesc: 'asc',
          isAggregateFn: undefined,
          formatColumnFn: undefined,
        }
      );
      expect(result).toEqual(nodes.slice(0, 5));
    });
    it('id desc', async () => {
      const nodes = factory.buildList(10).sort((a, b) => a.id - b.id);
      const edges = convertNodesToEdges(nodes, undefined, {
        orderColumn: 'id',
        primaryKey: 'id',
        ascOrDesc: 'desc',
      });
      await db('test_table').insert(nodes);
      const result = await removeNodesAfterAndIncluding(
        db('test_table'),
        edges[5].cursor,
        {
          orderColumn: 'id',
          primaryKey: 'id',
          ascOrDesc: 'desc',
          isAggregateFn: undefined,
          formatColumnFn: undefined,
        }
      );

      expect(result).toEqual(nodes.slice(6));
    });
  });

  it('removeNodesFromEnd', async () => {
    const nodes = factory.buildList(10).sort((a, b) => a.id - b.id);
    await db('test_table').insert(nodes);
    const result = await removeNodesFromEnd(db('test_table'), 3);
    expect(result).toEqual(nodes.slice(0, 3));
  });

  describe('removeNodesFromBeginning', () => {
    it('id asc', async () => {
      const nodes = factory.buildList(10).sort((a, b) => b.id - a.id);
      await db('test_table').insert(nodes);
      const result = await removeNodesFromBeginning(db('test_table'), 3, {
        orderColumn: 'id',
        primaryKey: 'id',
        ascOrDesc: 'asc',
      });

      expect(result).toEqual(nodes.slice(0, 3));
    });
    it('id desc', async () => {
      const nodes = factory.buildList(10).sort((a, b) => a.id - b.id);
      await db('test_table').insert(nodes);
      const result = await removeNodesFromBeginning(db('test_table'), 3, {
        orderColumn: 'id',
        primaryKey: 'id',
        ascOrDesc: 'desc',
      });

      expect(result).toEqual(nodes.slice(0, 3));
    });
  });

  describe('paginate', () => {
    let nodes: TestNode[];
    let edges: Edge<TestNode>[];

    beforeEach(async () => {
      nodes = factory.buildList(10).sort((a, b) => a.id - b.id);
      edges = convertNodesToEdges(nodes, undefined, {
        orderColumn: 'id',
        primaryKey: 'id',
        ascOrDesc: 'asc',
      });
      await db('test_table').insert(nodes);
    });

    describe('backwards pagination', () => {
      it('brings last 2 at the end', async () => {
        const result = await paginate(db('test_table'), {
          last: 2,
        });
        expect(result).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: false,
            hasPreviousPage: true,
            startCursor: edges[8].cursor,
            endCursor: edges[9].cursor,
          },
          edges: edges.slice(8, 10),
        });
      });

      it('brings last 2 in the middle', async () => {
        const result = await paginate(db('test_table'), {
          last: 2,
          before: edges[8].cursor,
        });
        expect(result).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: true,
            startCursor: edges[6].cursor,
            endCursor: edges[7].cursor,
          },
          edges: edges.slice(6, 8),
        });
      });
      it('brings last 2 at the beginning', async () => {
        const result = await paginate(db('test_table'), {
          last: 2,
          before: edges[2].cursor,
        });
        expect(result).toEqual({
          edges: edges.slice(0, 2),
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[0].cursor,
            endCursor: edges[1].cursor,
          },
        });
      });
    });
    describe('forward pagination', () => {
      it('brings first 2 at the beginning', async () => {
        const result = await paginate(db('test_table'), {
          first: 2,
        });
        expect(result).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[0].cursor,
            endCursor: edges[1].cursor,
          },
          edges: edges.slice(0, 2),
        });
      });

      it('brings first 2 in the middle', async () => {
        const result = await paginate(db('test_table'), {
          first: 2,
          after: edges[1].cursor,
        });
        expect(result).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: true,
            startCursor: edges[2].cursor,
            endCursor: edges[3].cursor,
          },
          edges: edges.slice(2, 4),
        });
      });
      it('brings first 2 at the end', async () => {
        const result = await paginate(db('test_table'), {
          first: 2,
          after: edges[7].cursor,
        });
        expect(result).toEqual({
          edges: edges.slice(8, 10),
          totalCount: 10,
          pageInfo: {
            hasNextPage: false,
            hasPreviousPage: true,
            startCursor: edges[8].cursor,
            endCursor: edges[9].cursor,
          },
        });
      });
    });

    describe('cursor stability', () => {
      it('remains stable after adding an element to the list', async () => {
        const result = await paginate(db('test_table'), {
          first: 3,
        });
        const { cursor } = result.edges[0];

        await db('test_table').insert({ id: 0 });

        const result2 = await paginate(db('test_table'), {
          first: 3,
        });
        const newCursor = result2.edges[1].cursor;
        expect(cursor).toEqual(newCursor);
      });
    });

    describe('sorting', () => {
      it('sorts asc and desc correctly by id', async () => {
        const result = await paginate(db('test_table'), {
          first: 3,
          orderBy: 'id',
          orderDirection: 'asc',
        });

        expect(result).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[0].cursor,
            endCursor: edges[2].cursor,
          },
          edges: edges.slice(0, 3),
        });

        const result2 = await paginate(db('test_table'), {
          first: 3,
          orderBy: 'id',
          orderDirection: 'desc',
        });

        expect(result2).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[9].cursor,
            endCursor: edges[7].cursor,
          },
          edges: [edges[9], edges[8], edges[7]],
        });
      });

      it('can sort by aggregate value', async () => {
        const result = await paginate(
          db('test_table')
            .sum('id as idsum')
            .select('test_table.*')
            .groupBy('id'),
          {
            first: 3,
            orderBy: 'idsum',
            orderDirection: 'asc',
          },
          {
            isAggregateFn: (column) => column === 'idsum',
            formatColumnFn: (column) =>
              column === 'idsum' ? db.raw('sum(id)') : column,
          }
        );

        expect(result).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[0].cursor,
            endCursor: edges[2].cursor,
          },
          edges: edges.slice(0, 3).map((e) => ({
            ...e,
            node: {
              ...e.node,
              idsum: e.node.id,
            },
          })),
        });

        const result2 = await paginate(
          db('test_table')
            .sum('id as idsum')
            .select('test_table.*')
            .groupBy('id'),
          {
            first: 1,
            after: edges[2].cursor,
            orderBy: 'idsum',
            orderDirection: 'asc',
          },
          {
            isAggregateFn: (column) => column === 'idsum',
            formatColumnFn: (column) =>
              column === 'idsum' ? db.raw('sum(id)') : column,
          }
        );

        expect(result2).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: true,
            startCursor: edges[3].cursor,
            endCursor: edges[3].cursor,
          },
          edges: edges.slice(3, 4).map((e) => ({
            ...e,
            node: {
              ...e.node,
              idsum: e.node.id,
            },
          })),
        });
      });

      it('sorts asc and desc correctly when result set is segmented', async () => {
        const result = await paginate(db('test_table'), {
          first: 2,
          orderBy: 'id',
          orderDirection: 'desc',
        });

        expect(result).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[9].cursor,
            endCursor: edges[8].cursor,
          },
          edges: edges.reverse().slice(0, 2),
        });

        const result2 = await paginate(db('test_table'), {
          first: 1,
          after: result.edges[1].cursor,
          orderBy: 'id',
          orderDirection: 'desc',
        });

        expect(result2).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: true,
            startCursor: edges[2].cursor,
            endCursor: edges[2].cursor,
          },
          edges: edges.slice(2, 3),
        });
      });

      it('sorts correctly when sorting by a non unique column and it gets segmentated', async () => {
        const additionalNode = factory.build({
          name: nodes[0].name,
        });
        nodes.push(additionalNode);
        nodes = nodes.sort((a, b) =>
          a.name !== b.name
            ? (a.name ?? '').localeCompare(b.name ?? '')
            : a.id - b.id
        );
        edges = convertNodesToEdges(nodes, undefined, {
          orderColumn: 'name',
          primaryKey: 'id',
          ascOrDesc: 'asc',
        });
        await db('test_table').insert(additionalNode);

        const result = await paginate(db('test_table'), {
          first: 2,
          orderBy: 'name',
          orderDirection: 'asc',
        });

        expect(result).toEqual({
          totalCount: 11,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[0].cursor,
            endCursor: edges[1].cursor,
          },
          edges: edges.slice(0, 2),
        });

        const result2 = await paginate(db('test_table'), {
          first: 2,
          after: result.edges[1].cursor,
          orderBy: 'name',
          orderDirection: 'asc',
        });

        expect(result2).toEqual({
          totalCount: 11,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: true,
            startCursor: edges[2].cursor,
            endCursor: edges[3].cursor,
          },
          edges: edges.slice(2, 4),
        });
      });

      it('can sort by multiple columns', async () => {
        const additionalNode = factory.build({
          name: nodes[0].name,
        });
        nodes.push(additionalNode);
        nodes = nodes.sort((a, b) =>
          a.name !== b.name
            ? (a.name ?? '').localeCompare(b.name ?? '')
            : a.age - b.age
        );
        edges = convertNodesToEdges(nodes, undefined, {
          orderColumn: ['name', 'age'],
          primaryKey: 'id',
          ascOrDesc: ['asc', 'asc'],
        });
        await db('test_table').insert(additionalNode);

        const result = await paginate(db('test_table'), {
          first: 2,
          orderBy: ['name', 'age'],
          orderDirection: ['asc', 'asc'],
        });

        expect(result).toEqual({
          totalCount: 11,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[0].cursor,
            endCursor: edges[1].cursor,
          },
          edges: edges.slice(0, 2),
        });

        const result2 = await paginate(db('test_table'), {
          first: 2,
          after: result.edges[1].cursor,
          orderBy: ['name', 'age'],
          orderDirection: ['asc', 'asc'],
        });

        expect(result2).toEqual({
          totalCount: 11,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: true,
            startCursor: edges[2].cursor,
            endCursor: edges[3].cursor,
          },
          edges: edges.slice(2, 4),
        });
      });

      it('can sort by multiple columns using reverse pagination', async () => {
        const additionalNode = factory.build({
          name: nodes[0].name,
        });
        nodes.push(additionalNode);
        nodes = nodes.sort((a, b) =>
          a.name !== b.name
            ? (a.name ?? '').localeCompare(b.name ?? '')
            : a.age - b.age
        );
        edges = convertNodesToEdges(nodes, undefined, {
          orderColumn: ['name', 'age'],
          primaryKey: 'id',
          ascOrDesc: 'asc',
        });
        await db('test_table').insert(additionalNode);

        const result = await paginate(db('test_table'), {
          last: 2,
          orderBy: ['name', 'age'],
          orderDirection: ['asc', 'asc'],
        });

        expect(result).toEqual({
          totalCount: 11,
          pageInfo: {
            hasNextPage: false,
            hasPreviousPage: true,
            startCursor: edges[9].cursor,
            endCursor: edges[10].cursor,
          },
          edges: edges.slice(9, 11),
        });

        const result2 = await paginate(db('test_table'), {
          last: 2,
          before: result.edges[0].cursor,
          orderBy: ['name', 'age'],
          orderDirection: ['asc', 'asc'],
        });

        expect(result2).toEqual({
          totalCount: 11,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: true,
            startCursor: edges[7].cursor,
            endCursor: edges[8].cursor,
          },
          edges: edges.slice(7, 9),
        });
      });
    });

    describe('totalCount', () => {
      it('brings the correct amount for a non-segmented query', async () => {
        const result = await paginate(db('test_table'), {
          first: 2,
        });

        expect(result.totalCount).toEqual(10);
      });

      it('paginates segmentating by null values', async () => {
        nodes = nodes.sort((a, b) =>
          a.name === b.name
            ? a.id - b.id
            : (a.name ?? '').localeCompare(b.name ?? '')
        );

        await db('test_table')
          .update({ name: null })
          .where('id', nodes[0].id)
          .returning('*');

        nodes[0].name = null;

        edges = convertNodesToEdges(nodes, undefined, {
          orderColumn: 'name',
          primaryKey: 'id',
          ascOrDesc: 'asc',
        });

        const result = await paginate(db('test_table'), {
          first: 2,
          orderBy: 'name',
          orderDirection: 'asc',
        });

        expect(result).toEqual({
          edges: edges.slice(0, 2),
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[0].cursor,
            endCursor: edges[1].cursor,
          },
        });

        const result2 = await paginate(db('test_table'), {
          first: 2,
          after: result.edges[1].cursor,
          orderBy: 'name',
          orderDirection: 'asc',
        });

        expect(result2).toEqual({
          edges: edges.slice(2, 4),
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: true,
            startCursor: edges[2].cursor,
            endCursor: edges[3].cursor,
          },
        });
      });

      it('paginates segmentating in the middle of null values', async () => {
        nodes = nodes.sort((a, b) =>
          a.name === b.name
            ? a.id - b.id
            : (a.name ?? '').localeCompare(b.name ?? '')
        );

        await db('test_table')
          .update({ name: null })
          .where('id', nodes[1].id)
          .returning('*')
          .then(([x]) => x);
        await db('test_table')
          .update({ name: null })
          .where('id', nodes[2].id)
          .returning('*')
          .then(([x]) => x);

        nodes[1].name = null;
        nodes[2].name = null;

        nodes = nodes.sort((a, b) =>
          a.name === b.name
            ? a.id - b.id
            : (a.name ?? '').localeCompare(b.name ?? '')
        );

        edges = convertNodesToEdges(nodes, undefined, {
          orderColumn: 'name',
          primaryKey: 'id',
          ascOrDesc: 'asc',
        });

        const result = await paginate(db('test_table'), {
          first: 1,
          orderBy: 'name',
          orderDirection: 'asc',
        });

        expect(result).toEqual({
          edges: edges.slice(0, 1),
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[0].cursor,
            endCursor: edges[0].cursor,
          },
        });

        const result2 = await paginate(db('test_table'), {
          first: 2,
          after: result.edges[0].cursor,
          orderBy: 'name',
          orderDirection: 'asc',
        });

        expect(result2).toEqual({
          edges: edges.slice(1, 3),
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: true,
            startCursor: edges[1].cursor,
            endCursor: edges[2].cursor,
          },
        });
      });
    });

    describe('modifyEdgeFn', () => {
      it('modifies edges per the callback', async () => {
        const result = await paginate(
          db('test_table'),
          {
            first: 2,
          },
          {
            modifyEdgeFn(edge) {
              return {
                ...edge,
                custom: 'test',
              };
            },
          }
        );
        expect(result).toEqual({
          totalCount: 10,
          pageInfo: {
            hasNextPage: true,
            hasPreviousPage: false,
            startCursor: edges[0].cursor,
            endCursor: edges[1].cursor,
          },
          edges: edges.slice(0, 2).map((edge) => ({
            ...edge,
            custom: 'test',
          })),
        });
      });
    });
  });
});
