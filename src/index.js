const { ApolloServer, gql } = require('apollo-server');
const DataLoader = require('dataloader');

const typeDefs = gql`
  type User {
    id: Int!
    name: String
    email: String
    last_name: String
    birthday: String  
    address: String
    email_verified_at: String
    password: String
    remember_token: String
    created_at: String
    updated_at: String
    posts: [Post]
  }

  type Post {
    post_id: Int
    user_id: Int
    title: String
    description: String
    created_at: String
    updated_at: String	
    comments: [Comment]
  }

  type Comment {
    comment_id: Int
    description: String
    post_id: Int
    user_id: Int
    created_at: String
    updated_at: String
  }

  type Query {
    users(first: Int): [User]
  }

  input ClientInput {
    name: String
    email: String
    last_name: String
    birthday: String  
    address: String   
    created_at: String!
  }

  type Client {
    id: Int!
    name: String
    email: String
    last_name: String
    birthday: String  
    address: String   
    created_at: String!
    updated_at: String
  }

  type Mutation {
    createClient(client: ClientInput!): Client!
  }
`;

const getKnexClient = () => {
  return require('knex')({
    client: 'pg',
    connection: {
      host: '127.0.0.1',
      user: 'postgres',
      password: '123304050',
      port: 5432,
      database: 'blogdb_v1'
    }
  });
}
const knex = getKnexClient();

const getFields = (info, fieldNodeName) => {
  const postsFieldNode = info.fieldNodes.find((fn) => fn.name.value === fieldNodeName);
  return postsFieldNode.selectionSet.selections.map((s) => s.name.value);
}
// A map of functions which return data for the schema.
const resolvers = {
  Query: {
    users: async (_, args, context, info) => {
      const startTime = new Date().getTime();
      const allowedColumns = ['id', 'name', 'last_name', 'email', 'address', 'email_verified_at', 'password', 'remember_token', 'created_at', 'updated_at']
      const fields = getFields(info, 'users').filter((f) => allowedColumns.indexOf(f) !== -1);
      console.log('Starting getting users...');
      const users = await knex.table('users')
        .select(fields.length > 0 ? fields : ['id'])
        .limit(args.first || 10)
        .orderBy('id')
        .transacting(context.tx);
      console.log('Users gotten!!!', users.length, `Time: ${(new Date().getTime() - startTime) / 1000}`);
      return users;
    },
  },
  User: {
    posts: async (parent, args, context, info) => {
      return context.dataLoaders.userPosts.load(parent.id);
    }
  },
  Post: {
    comments: async (parent, args, context, info) => {
      return context.dataLoaders.postComments.load(parent.post_id);
    }
  },
  Mutation: {
    createClient: async (_, args, context) => {
      try {
        const { client } = args;
        const result = await knex.table('clients').insert(client).returning('id').transacting(context.tx);
        return { id: result[0], ...client };
      } catch (error) {
        console.error('Error creating client', error);
        throw error;
      }
    }
  }
};

const TXPlugin = {
  requestDidStart() {
    console.log('REQUEST DID START');
    const startTime = new Date().getTime();
    return {
      async willSendResponse(args, b, c) {
        console.log('TOTAL TIME (TX PLUGIN): ', (new Date().getTime() - startTime) / 1000)
        if (args.errors) {
          console.info('Rolling back transaction');
          await args.context.tx.rollback();
        } else {
          console.info('Committing transaction');
          await args.context.tx.commit();
        }
      }
    }
  },

};
const server = new ApolloServer({
  playground: false,
  typeDefs,
  resolvers,
  plugins: [
    TXPlugin
  ],
  context: async (_) => {
    const trans = await knex.transaction();
    return {
      tx: trans,
      dataLoaders: {
        userPosts: new DataLoader(async (userIds) => {
          try {
            const startTime = new Date().getTime();
            const userPostsObj = userIds.reduce((prev, userId) => {
              prev[userId] = [];
              return prev;
            }, {});

            console.log(`Getting posts for ${userIds.length} users...`);
            const promises = [];
            const size = 50000;
            for (let i = 0; i < userIds.length; i = i + size) {
              // const tempUserIds = userIds.slice(i, i + size).map(id => `(${id})`);
              const tempUserIds = userIds.slice(i, i + size);
              console.log('Getting posts...', i, i + size);
              // console.log(`SELECT * from posts WHERE user_id = ANY (VALUES ${tempUserIds.join(`,`)})`);
              promises.push(
                // knex.raw(`SELECT * from posts WHERE user_id = ANY (VALUES ${tempUserIds.join(`,`)})`).transacting(trans).then((r) => r.rows)
                knex.raw(`SELECT * from posts WHERE user_id IN (${tempUserIds.join(',')})`).transacting(trans).then((r) => r.rows)
              );
            }
            console.log('Waiting for all DB posts...');
            const promiseWaitingStartTime = new Date().getTime();
            const promisesResult = await Promise.all(promises);

            console.log('All posts gotten from DB', (new Date().getTime() - promiseWaitingStartTime) / 1000);

            console.log('Preparing posts...');
            const prepareStartTime = new Date().getTime();
            promisesResult.forEach((tempPosts) => {
              tempPosts.forEach((post) => {
                userPostsObj[post.user_id].push(post);
              });
            });

            const result = userIds.map((userId) => userPostsObj[userId]);
            console.log('Posts prepared time', (new Date().getTime() - prepareStartTime) / 1000)
            console.log('Total POSTS time', (new Date().getTime() - startTime) / 1000);
            return result;
          } catch (error) {
            console.error('Error getting posts!!!!', error);
            throw error;
          }
        }, { cache: false }),
        postComments: new DataLoader(async (postIds) => {
          try {
            const startTime = new Date().getTime();
            const postCommentsObj = postIds.reduce((prev, postId) => {
              prev[postId] = [];
              return prev;
            }, {});
            console.log(`Getting comments of ${postIds.length} posts...`);
            const promises = [];
            const size = 60000;
            for (let i = 0; i < postIds.length; i = i + size) {
              // const tempPostIds = postIds.slice(i, i + size).map(id => `(${id})`);
              const tempPostIds = postIds.slice(i, i + size);
              console.log('Getting comments...', i, i + size);
              promises.push(
                // knex.raw(`SELECT * from comments WHERE post_id = ANY (VALUES ${tempPostIds.join(`,`)})`).transacting(trans).then((r) => r.rows)
                knex.raw(`SELECT * from comments WHERE post_id IN (${tempPostIds.join(`,`)})`).transacting(trans).then((r) => r.rows)
              );
            }
            console.log('Waiting for all DB comments...');
            const promiseWaitingStartTime = new Date().getTime();
            const promisesResult = await Promise.all(promises);
            console.log('Comments gotten from DB!!!', (new Date().getTime() - promiseWaitingStartTime) / 1000)
            console.log('Preparing comments...');
            const prepareStartTime = new Date().getTime();
            promisesResult.forEach((tempComments) => {
              tempComments.forEach((comment) => {
                postCommentsObj[comment.post_id].push(comment);
              });
            });

            const result = postIds.map((postId) => postCommentsObj[postId]);
            console.log('Comments prepared!!!', (new Date().getTime() - prepareStartTime) / 1000);
            console.log('Total COMMENTS time', (new Date().getTime() - startTime) / 1000);
            return result;
          } catch (error) {
            console.error('Error getting comments!!!!', error);
            throw error;
          }
        }, { cache: false }),
      },
    };
  },
});

server.listen().then(({ url }) => {
  console.log(`🚀 Server ready at ${url}`);
});
