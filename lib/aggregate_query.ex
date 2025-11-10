# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs.contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.AggregateQuery do
  @moduledoc false
  import Ecto.Query, only: [from: 2, subquery: 1]

  def run_aggregate_query(original_query, aggregates, resource, implementation) do
    original_query =
      AshSql.Bindings.default_bindings(original_query, resource, implementation)

    # Check if we have bypass aggregates with context multitenancy
    bypass_aggregates =
      Enum.filter(aggregates, fn agg ->
        Map.get(agg, :multitenancy) == :bypass &&
          Ash.Resource.Info.multitenancy_strategy(resource) == :context
      end)

    case bypass_aggregates do
      [] ->
        # No bypass aggregates, use normal path
        run_normal_aggregate_query(original_query, aggregates, resource, implementation)

      _ ->
        # Have bypass aggregates with context multitenancy
        # Query across all tenants and combine results
        run_bypass_aggregate_query(original_query, aggregates, resource, implementation)
    end
  end

  defp run_normal_aggregate_query(original_query, aggregates, resource, implementation) do
    {can_group, cant_group} =
      aggregates
      |> Enum.split_with(&AshSql.Aggregate.can_group?(resource, &1, original_query))
      |> case do
        {[one], cant_group} -> {[], [one | cant_group]}
        {can_group, cant_group} -> {can_group, cant_group}
      end

    {global_filter, can_group} =
      AshSql.Aggregate.extract_shared_filters(can_group)

    query =
      case global_filter do
        {:ok, global_filter} ->
          AshSql.Filter.filter(original_query, global_filter, resource)

        :error ->
          {:ok, original_query}
      end

    case query do
      {:error, error} ->
        {:error, error}

      {:ok, query} ->
        query =
          if query.distinct || query.limit do
            query =
              query
              |> Ecto.Query.exclude(:select)
              |> Ecto.Query.exclude(:order_by)
              |> Map.put(:windows, [])

            from(row in subquery(query), as: ^query.__ash_bindings__.root_binding, select: %{})
          else
            query
            |> Ecto.Query.exclude(:select)
            |> Ecto.Query.exclude(:order_by)
            |> Map.put(:windows, [])
            |> Ecto.Query.select(%{})
          end
          |> Map.put(:__ash_bindings__, query.__ash_bindings__)

        group_query =
          Enum.reduce(
            can_group,
            query,
            fn agg, query ->
              first_relationship =
                Ash.Resource.Info.relationship(resource, agg.relationship_path |> Enum.at(0))

              AshSql.Aggregate.add_subquery_aggregate_select(
                query,
                agg.relationship_path |> Enum.drop(1),
                agg,
                resource,
                false,
                first_relationship
              )
            end
          )

        result =
          case can_group do
            [] ->
              %{}

            _ ->
              repo = AshSql.dynamic_repo(resource, implementation, query)
              repo.one(group_query, AshSql.repo_opts(repo, implementation, nil, nil, resource))
          end

        {:ok, add_single_aggs(result, resource, query, cant_group, implementation)}
    end
  end

  def add_single_aggs(result, resource, query, cant_group, implementation) do
    Enum.reduce(cant_group, result, fn
      %{kind: :exists} = agg, result ->
        # Note: Bypass EXISTS aggregates are handled at the query execution level
        # This just handles normal EXISTS aggregates
        {:ok, filtered} =
          case agg do
            %{query: %{filter: filter}} when not is_nil(filter) ->
              AshSql.Filter.filter(query, filter, resource)

            _ ->
              {:ok, query}
          end

        filtered =
          if filtered.distinct || filtered.limit do
            filtered =
              filtered
              |> Ecto.Query.exclude(:select)
              |> Ecto.Query.exclude(:order_by)
              |> Map.put(:windows, [])

            from(row in subquery(filtered), as: ^query.__ash_bindings__.root_binding, select: %{})
          else
            filtered
            |> Ecto.Query.exclude(:select)
            |> Ecto.Query.exclude(:order_by)
            |> Map.put(:windows, [])
            |> Ecto.Query.select(%{})
          end

        repo = AshSql.dynamic_repo(resource, implementation, filtered)

        Map.put(
          result || %{},
          agg.name,
          repo.exists?(filtered, AshSql.repo_opts(repo, implementation, nil, nil, resource))
        )

      agg, result ->
        {:ok, filtered} =
          case agg do
            %{query: %{filter: filter}} when not is_nil(filter) ->
              AshSql.Filter.filter(query, filter, resource)

            _ ->
              {:ok, query}
          end

        filtered =
          if filtered.distinct do
            in_query = filtered |> Ecto.Query.exclude(:distinct) |> Ecto.Query.exclude(:select)

            dynamic =
              Enum.reduce(Ash.Resource.Info.primary_key(resource), nil, fn key, dynamic ->
                if dynamic do
                  Ecto.Query.dynamic(
                    [row],
                    ^dynamic and
                      field(parent_as(^query.__ash_bindings__.root_binding), ^key) ==
                        field(row, ^key)
                  )
                else
                  Ecto.Query.dynamic(
                    [row],
                    field(parent_as(^query.__ash_bindings__.root_binding), ^key) ==
                      field(row, ^key)
                  )
                end
              end)

            in_query =
              from(row in in_query, where: ^dynamic)

            in_query = Ecto.Query.exclude(in_query, :distinct)

            from(row in query.from.source,
              as: ^query.__ash_bindings__.root_binding,
              where: exists(in_query)
            )
          else
            filtered
          end

        filtered =
          if filtered.limit do
            filtered =
              filtered
              |> Ecto.Query.exclude(:select)
              |> Ecto.Query.exclude(:order_by)
              |> Map.put(:windows, [])

            from(row in subquery(filtered), as: ^query.__ash_bindings__.root_binding, select: %{})
          else
            filtered
            |> Ecto.Query.exclude(:select)
            |> Ecto.Query.exclude(:order_by)
            |> Map.put(:windows, [])
            |> Ecto.Query.select(%{})
          end

        first_relationship =
          Ash.Resource.Info.relationship(resource, agg.relationship_path |> Enum.at(0))

        filtered = AshSql.Bindings.default_bindings(filtered, resource, implementation)

        ref =
          AshSql.Aggregate.aggregate_field_ref(
            agg,
            Ash.Resource.Info.related(resource, agg.relationship_path),
            agg.relationship_path,
            filtered,
            first_relationship
          )

        {:ok, filtered} =
          if ref do
            {:ok, filtered} =
              case ref.attribute do
                %struct{} = agg when struct in [Ash.Query.Aggregate, Ash.Resource.Aggregate] ->
                  AshSql.Aggregate.add_aggregates(
                    filtered,
                    [agg],
                    resource,
                    false,
                    filtered.__ash_bindings__.root_binding
                  )

                %Ash.Query.Calculation{} = calc ->
                  used_aggregates = Ash.Filter.used_aggregates(calc, [])

                  with {:ok, filtered} <- AshSql.Join.join_all_relationships(filtered, calc, []) do
                    AshSql.Aggregate.add_aggregates(
                      filtered,
                      used_aggregates,
                      resource,
                      false,
                      filtered.__ash_bindings__.root_binding
                    )
                  end

                _other ->
                  {:ok, filtered}
              end

            AshSql.Join.join_all_relationships(filtered, ref)
          else
            {:ok, filtered}
          end

        query =
          AshSql.Aggregate.add_subquery_aggregate_select(
            filtered,
            agg.relationship_path |> Enum.drop(1),
            %{agg | query: %{agg.query | filter: nil}},
            resource,
            true,
            first_relationship
          )

        repo = AshSql.dynamic_repo(resource, implementation, query)

        Map.merge(
          result || %{},
          repo.one(
            query,
            AshSql.repo_opts(repo, query.__ash_bindings__.sql_behaviour, nil, nil, resource)
          )
        )
    end)
  end

  defp run_bypass_aggregate_query(original_query, aggregates, resource, implementation) do
    repo = AshSql.dynamic_repo(resource, implementation, original_query)

    # Get all tenants to query across
    all_tenants =
      if repo && function_exported?(repo, :all_tenants, 0) do
        repo.all_tenants()
      else
        []
      end

    # For bypass aggregates with context multitenancy, we need to let the database
    # aggregate across all tenant schemas. We'll build SQL fragments that use UNION ALL
    # to combine data from all schemas, then apply aggregates on top.

    result =
      Enum.reduce(aggregates, %{}, fn agg, acc ->
        # Get the related resource that the aggregate operates on
        related_resource =
          case agg.relationship_path do
            [] -> resource
            path -> Ash.Resource.Info.related(resource, path)
          end

        # Build the aggregate query across all tenant schemas using SQL
        value = compute_bypass_aggregate(agg, related_resource, all_tenants, repo, implementation)
        Map.put(acc, agg.name, value)
      end)

    {:ok, result}
  end

  defp compute_bypass_aggregate(agg, related_resource, all_tenants, repo, implementation) do
    # Get the table name from the resource's data layer info
    # For AshPostgres.SqlImplementation, the Info module is AshPostgres.DataLayer.Info
    # Take only the first part (e.g., "AshPostgres") and append "DataLayer.Info"
    table =
      implementation
      |> Module.split()
      |> Enum.take(1)
      |> Kernel.++(["DataLayer", "Info"])
      |> Module.concat()
      |> apply(:table, [related_resource])

    case agg.kind do
      :count ->
        # Build SQL: SELECT SUM(cnt) FROM (SELECT COUNT(*) as cnt FROM schema1.table UNION ALL SELECT COUNT(*) FROM schema2.table ...)
        union_sql =
          all_tenants
          |> Enum.map(fn tenant ->
            "(SELECT COUNT(*) as cnt FROM \"#{tenant}\".\"#{table}\")"
          end)
          |> Enum.join(" UNION ALL ")

        sql = "SELECT COALESCE(SUM(cnt), 0) as total FROM (#{union_sql}) AS counts"

        case repo.query(sql, [], AshSql.repo_opts(repo, implementation, nil, nil, related_resource)) do
          {:ok, %{rows: [[count]]}} -> count
          _ -> 0
        end

      :exists ->
        # Build SQL: SELECT EXISTS(SELECT 1 FROM schema1.table UNION ALL SELECT 1 FROM schema2.table LIMIT 1)
        union_sql =
          all_tenants
          |> Enum.map(fn tenant ->
            "(SELECT 1 FROM \"#{tenant}\".\"#{table}\" LIMIT 1)"
          end)
          |> Enum.join(" UNION ALL ")

        sql = "SELECT EXISTS(#{union_sql})"

        case repo.query(sql, [], AshSql.repo_opts(repo, implementation, nil, nil, related_resource)) do
          {:ok, %{rows: [[exists]]}} -> exists
          _ -> false
        end

      :list ->
        # Build SQL: SELECT field FROM schema1.table UNION ALL SELECT field FROM schema2.table ...
        field = agg.field

        union_sql =
          all_tenants
          |> Enum.map(fn tenant ->
            "(SELECT \"#{field}\" FROM \"#{tenant}\".\"#{table}\")"
          end)
          |> Enum.join(" UNION ALL ")

        sql = "SELECT * FROM (#{union_sql}) AS all_values"

        case repo.query(sql, [], AshSql.repo_opts(repo, implementation, nil, nil, related_resource)) do
          {:ok, %{rows: rows}} -> Enum.map(rows, fn [val] -> val end)
          _ -> []
        end

      :sum ->
        # Build SQL: SELECT SUM(total) FROM (SELECT SUM(field) as total FROM schema1.table UNION ALL SELECT SUM(field) FROM schema2.table ...)
        field = agg.field

        union_sql =
          all_tenants
          |> Enum.map(fn tenant ->
            "(SELECT COALESCE(SUM(\"#{field}\"), 0) as total FROM \"#{tenant}\".\"#{table}\")"
          end)
          |> Enum.join(" UNION ALL ")

        sql = "SELECT COALESCE(SUM(total), 0) as grand_total FROM (#{union_sql}) AS sums"

        case repo.query(sql, [], AshSql.repo_opts(repo, implementation, nil, nil, related_resource)) do
          {:ok, %{rows: [[sum]]}} -> sum
          _ -> 0
        end

      _ ->
        # For other aggregate types, return nil
        nil
    end
  end
end
