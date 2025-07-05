import {
  describe,
  expect,
  it,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
} from '@jest/globals';
import { faker } from '@faker-js/faker';
import { Entity, item, number, string, Table } from 'dynamodb-toolbox';
import {
  CreateTableCommandInput,
  CreateTableCommand,
  DeleteTableCommand,
  PutItemCommand,
  DeleteItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb';
import { documentClient } from '../client/aws/ddb';
import paginate from '..';
import { encode, decode } from '../../../builder';

const testTableName = 'test-table';
// Table configuration
const tableConfig = (TableName: string): CreateTableCommandInput => ({
  TableName,
  KeySchema: [
    {
      KeyType: 'HASH',
      AttributeName: 'pk',
    },
    {
      KeyType: 'RANGE',
      AttributeName: 'sk',
    },
  ],
  AttributeDefinitions: [
    {
      AttributeName: 'pk',
      AttributeType: 'S',
    },
    {
      AttributeName: 'sk',
      AttributeType: 'S',
    },
  ],
  BillingMode: 'PAY_PER_REQUEST',
});

// Table creation and deletion functions
const createTable = async (tableName: string) => {
  await documentClient.send(new CreateTableCommand(tableConfig(tableName)));
};

const deleteTable = async (tableName: string) => {
  await documentClient.send(new DeleteTableCommand({ TableName: tableName }));
};

const table = new Table({
  name: testTableName,
  partitionKey: { name: 'pk', type: 'string' },
  sortKey: { name: 'sk', type: 'string' },
  documentClient,
  indexes: {
    inverse: {
      type: 'global',
      partitionKey: { name: 'sk', type: 'string' },
      sortKey: { name: 'pk', type: 'string' },
    },
    gsi2: {
      type: 'global',
      partitionKey: { name: 'pk2', type: 'string' },
      sortKey: { name: 'sk2', type: 'string' },
    },
    inverse2: {
      type: 'global',
      partitionKey: { name: 'sk2', type: 'string' },
      sortKey: { name: 'pk2', type: 'string' },
    },
  },
});

const TestEntity = new Entity({
  name: 'TestEntity',
  schema: item({
    pk: string().key(),
    sk: string().key(),
    name: string(),
    age: number(),
    createdAt: string(),
    category: string(),
  }),
  table,
});

describe('DynamoDB Toolbox Pagination', () => {
  beforeEach(async () => {
    // Create test table
    await createTable(testTableName);
  });

  afterEach(async () => {
    await deleteTable(testTableName);
  });

  describe('Utility Functions', () => {
    it('should encode and decode strings correctly', () => {
      const testString = 'test-string';
      const encoded = encode(testString);
      const decoded = decode(encoded);
      expect(decoded).toBe(testString);
    });

    it('should handle complex data in encoding', () => {
      const complexData = {
        id: 123,
        name: 'test',
        nested: { value: 'nested' },
      };
      const encoded = encode(JSON.stringify(complexData));
      const decoded = decode(encoded);
      expect(JSON.parse(decoded)).toEqual(complexData);
    });
  });

  describe('Basic Pagination', () => {
    it('should return first N items when first is specified', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      const result = await paginator(
        query,
        { first: 5 },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(result.edges).toHaveLength(5);
      expect(result.pageInfo.hasNextPage).toBe(true);
      expect(result.pageInfo.hasPreviousPage).toBe(false);
      expect(result.totalCount).toBeGreaterThan(0);
    });

    it('should return last N items when last is specified', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      const result = await paginator(
        query,
        { last: 5 },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(result.edges).toHaveLength(5);
      expect(result.pageInfo.hasNextPage).toBe(false);
      expect(result.pageInfo.hasPreviousPage).toBe(true);
    });

    it('should handle empty results', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'nonexistent#', 'post#');

      const result = await paginator(
        query,
        { first: 10 },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(result.edges).toHaveLength(0);
      expect(result.pageInfo.hasNextPage).toBe(false);
      expect(result.pageInfo.hasPreviousPage).toBe(false);
      expect(result.totalCount).toBe(0);
    });
  });

  describe('Cursor-based Pagination', () => {
    it('should handle after cursor correctly', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      // Get first page
      const firstResult = await paginator(
        query,
        { first: 3 },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(firstResult.edges).toHaveLength(3);
      const afterCursor = firstResult.pageInfo.endCursor;

      // Get second page using after cursor
      const secondResult = await paginator(
        query,
        { first: 3, after: afterCursor },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(secondResult.edges).toHaveLength(3);
      expect(secondResult.pageInfo.hasPreviousPage).toBe(true);

      // Ensure no overlap between pages
      const firstPageIds = firstResult.edges.map(
        (edge) => (edge.node as any).pk + (edge.node as any).sk
      );
      const secondPageIds = secondResult.edges.map(
        (edge) => (edge.node as any).pk + (edge.node as any).sk
      );

      const intersection = firstPageIds.filter((id) =>
        secondPageIds.includes(id)
      );
      expect(intersection).toHaveLength(0);
    });

    it('should handle before cursor correctly', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      // Get first page
      const firstResult = await paginator(
        query,
        { first: 5 },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      // Get second page
      const secondResult = await paginator(
        query,
        { first: 5, after: firstResult.pageInfo.endCursor },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      // Go back to first page using before cursor
      const backToFirstResult = await paginator(
        query,
        { last: 5, before: secondResult.pageInfo.startCursor },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(backToFirstResult.edges).toHaveLength(5);
      expect(backToFirstResult.pageInfo.hasNextPage).toBe(true);
    });
  });

  describe('Ordering', () => {
    it('should order by createdAt in ascending order', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      const result = await paginator(
        query,
        {
          first: 10,
          orderBy: 'createdAt',
          orderDirection: 'asc',
        },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(result.edges).toHaveLength(10);

      // Check if items are ordered by createdAt ascending
      const dates = result.edges.map(
        (edge) => new Date((edge.node as any).createdAt)
      );
      for (let i = 1; i < dates.length; i++) {
        expect(dates[i].getTime()).toBeGreaterThanOrEqual(
          dates[i - 1].getTime()
        );
      }
    });

    it('should order by age in descending order', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      const result = await paginator(
        query,
        {
          first: 10,
          orderBy: 'age',
          orderDirection: 'desc',
        },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(result.edges).toHaveLength(10);

      // Check if items are ordered by age descending
      const ages = result.edges.map((edge) => (edge.node as any).age);
      for (let i = 1; i < ages.length; i++) {
        expect(ages[i]).toBeLessThanOrEqual(ages[i - 1]);
      }
    });
  });

  describe('Edge Cases', () => {
    it('should handle negative first parameter', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      await expect(
        paginator(query, { first: -1 }, { primaryKey: { pk: 'pk', sk: 'sk' } })
      ).rejects.toThrow('`first` argument must not be less than 0');
    });

    it('should handle negative last parameter', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      await expect(
        paginator(query, { last: -1 }, { primaryKey: { pk: 'pk', sk: 'sk' } })
      ).rejects.toThrow('`last` argument must not be less than 0');
    });

    it('should handle invalid cursor', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      await expect(
        paginator(
          query,
          { first: 5, after: 'invalid-cursor' },
          { primaryKey: { pk: 'pk', sk: 'sk' } }
        )
      ).rejects.toThrow('Could not find edge with cursor invalid-cursor');
    });

    it('should handle both first and last parameters', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      // This should prioritize 'first' over 'last'
      const result = await paginator(
        query,
        { first: 3, last: 5 },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(result.edges).toHaveLength(3);
    });
  });

  describe('Total Count', () => {
    it('should return total count when not skipped', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      const result = await paginator(
        query,
        { first: 5 },
        { primaryKey: { pk: 'pk', sk: 'sk' } }
      );

      expect(result.totalCount).toBeDefined();
      expect(result.totalCount).toBeGreaterThan(0);
    });

    it('should skip total count when specified', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      const result = await paginator(
        query,
        { first: 5 },
        {
          primaryKey: { pk: 'pk', sk: 'sk' },
          skipTotalCount: true,
        }
      );

      expect(result.totalCount).toBeUndefined();
    });
  });

  describe('Real DynamoDB Table Structure', () => {
    it('should work with the provided table structure', async () => {
      const paginator = paginate();
      const query = createMockQuery(testTableName, 'user#', 'post#');

      const result = await paginator(
        query,
        { first: 5 },
        {
          primaryKey: { pk: 'pk', sk: 'sk' },
          orderBy: 'createdAt',
          orderDirection: 'desc',
        }
      );

      expect(result.edges).toHaveLength(5);
      expect(result.pageInfo.hasNextPage).toBe(true);

      // Verify the cursor structure works with the table's primary key structure
      const firstCursor = result.edges[0].cursor;
      const decodedCursor = decode(firstCursor);
      expect(decodedCursor).toContain('_*_');

      // The cursor should contain both pk and sk values
      const [pk, sk] = decodedCursor.split('_*_');
      expect(pk).toBeTruthy();
      expect(sk).toBeTruthy();
    });
  });
});
