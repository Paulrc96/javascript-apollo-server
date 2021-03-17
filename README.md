# NodeJS Apollo Server

## Install dependencies
```
npm install
```

## Run server
```
npm run serve
```
## Query
URL: POST `http://localhost:4000`

Example query:
```graphql
query {
  users (first: 1) {
      id
      name
      last_name
      email
      address
      birthday
      posts {
        title
        description
        comments {
          post_id
          description
       }
     }
  }
}
```

## PostgreSQL

```SQL
CREATE INDEX posts__user_id_idx ON posts (user_id);

CREATE INDEX comments__user_id_idx ON comments (user_id);
CREATE INDEX comments__post_id_idx ON comments (post_id);
```