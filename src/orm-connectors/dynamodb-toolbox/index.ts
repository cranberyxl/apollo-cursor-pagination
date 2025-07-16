import {
  type Entity,
  type FormattedItem,
  EntityParser,
  InputValue,
  Schema,
  PrimaryKey,
  EntityAccessPattern,
} from 'dynamodb-toolbox';
import apolloCursorPaginationBuilder, {
  encode,
  decode,
  GraphQLParams,
  BuilderOptions,
} from '../../builder';

export const cursorGenerator = <E extends Entity>(
  key: PrimaryKey<E['table']>
): string => encode(JSON.stringify(key, Object.keys(key).sort()));

export const getDataFromCursor = (cursor: string) => JSON.parse(decode(cursor));

export const convertNodesToEdges =
  <N, ENTITY extends Entity = Entity, SCHEMA extends Schema = Schema>(
    queryInput: InputValue<SCHEMA>,
    accessPattern: EntityAccessPattern<ENTITY, SCHEMA>
  ) =>
  (nodes: N[]) =>
    nodes.map((node) => {
      const parsed = accessPattern.entity.build(EntityParser).parse(node);

      // Use the index info in the query to find all the keys
      const nodePrimaryKey: Record<string, any> = parsed.key;
      const queryParms = accessPattern.query(queryInput).params();
      if (queryParms.IndexName) {
        const index = accessPattern.entity.table.indexes[queryParms.IndexName];
        if (index.partitionKey) {
          nodePrimaryKey[index.partitionKey.name] =
            parsed.item[index.partitionKey.name];
        }
        if (index.sortKey) {
          nodePrimaryKey[index.sortKey.name] = parsed.item[index.sortKey.name];
        }
      }

      return {
        cursor: cursorGenerator(nodePrimaryKey),

        node,
      };
    });

export default function paginate<
  ENTITY extends Entity = Entity,
  SCHEMA extends Schema = Schema,
>(
  queryInput: InputValue<SCHEMA>,
  accessPattern: EntityAccessPattern<ENTITY, SCHEMA>,
  args?: Omit<GraphQLParams, 'orderBy'>,
  builderOptions?: BuilderOptions<undefined, FormattedItem<ENTITY>>
) {
  return apolloCursorPaginationBuilder<
    FormattedItem<ENTITY>,
    EntityAccessPattern<ENTITY, SCHEMA>,
    undefined
  >({
    applyAfterCursor: (nodeAccessor, afterCursor) => {
      const decodedCursor = getDataFromCursor(afterCursor);

      return nodeAccessor.options((previousOptions) => {
        if (previousOptions.exclusiveStartKey) {
          throw new Error(
            'exclusiveStartKey already set - cannot apply after cursor'
          );
        }

        return {
          ...previousOptions,
          exclusiveStartKey: decodedCursor,
        };
      });
    },
    applyBeforeCursor: (nodeAccessor, beforeCursor) => {
      const decodedCursor = getDataFromCursor(beforeCursor);

      return nodeAccessor.options((previousOptions) => {
        if (previousOptions.exclusiveStartKey) {
          throw new Error(
            'exclusiveStartKey already set - cannot apply before cursor'
          );
        }

        return {
          ...previousOptions,
          exclusiveStartKey: decodedCursor,
        };
      });
    },
    returnNodesForFirst: async (nodeAccessor, count, orderArgs) => {
      const result = await nodeAccessor
        .options((previousOptions) => {
          if ('limit' in previousOptions) {
            throw new Error('limit already set - cannot apply first');
          }

          if ('reverse' in previousOptions) {
            throw new Error(
              'reverse already set - cannot apply first, use orderDirection'
            );
          }

          return {
            ...previousOptions,
            limit: count,
            reverse: orderArgs.ascOrDesc === 'desc',
          };
        })
        .query(queryInput)
        .send();

      const items = (result.Items || []) as FormattedItem<ENTITY>[];
      return items;
    },
    returnNodesForLast: async (nodeAccessor, count, orderArgs) => {
      // For "last" parameter, we need to get the last N items from the end
      const result = await nodeAccessor
        .options((previousOptions) => {
          if ('limit' in previousOptions) {
            throw new Error('limit already set - cannot apply last');
          }

          if ('reverse' in previousOptions) {
            throw new Error(
              'reverse already set - cannot apply last, use orderDirection'
            );
          }

          return {
            ...previousOptions,
            limit: count,
            reverse: orderArgs.ascOrDesc === 'asc',
          };
        })
        .query(queryInput)
        .send();

      const items = (result.Items || []) as FormattedItem<ENTITY>[];

      return items.reverse();
    },
    returnTotalCount: async (nodeAccessor) => {
      const result = await nodeAccessor
        .options({ select: 'COUNT' })
        .query(queryInput)
        .send();
      return result.Count || 0;
    },
    convertNodesToEdges: convertNodesToEdges(queryInput, accessPattern),
    applyOrderBy: (nodeAccessor) => nodeAccessor,
  })(accessPattern as any, args, builderOptions ?? {});
}
