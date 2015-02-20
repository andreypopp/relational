# relational

## Example

Entity spec:

    let experiment = {
      entity: 'experiment',
      select: {
        title: true,
        date: true
      }
    }

    let study = {
      entity: 'study',
      select: {
        name: true,
        experiments: {
          spec: experiment,
          first: 10
        }
      }
    }

    let user = {
      entity: 'user',
      select: {
        username: true,
        firstName: 'first_name',
        lastName: 'last_name',
        studies: {
          spec: study,
          first: 10
        }
      }
    }

    fetch({
      spec: user,
      id: [1, 2, 3]
    })

    fetch({
      spec: study,
      first: 10
    })

Compiles to:

    WITH
      experiments AS (
        SELECT
          title,
          data,
          study_id
        FROM experiment
      ),
      studies AS (
        SELECT
          study.name,
          json_agg(experiments.*) as experiments,
          user_id
        FROM study
        JOIN experiments ON study.id = experiments.study_id
        GROUP BY study.id
        LIMIT 10
      )
    SELECT
      username,
      first_name as firstName,
      last_name as lastName,
      json_agg(study.*) as studies
    FROM user
    JOIN studies ON user.id = studies.user_id
    GROUP BY user.id

We know on which fields to join `user` and `study` and `study` and `experiment`
based on PostgreSQL table metadata. If there is ambigious situation, we issue a
warning.

## Example with facets

Facets are one-to-one relations, this is implemented similar to one-to-many
relations.
