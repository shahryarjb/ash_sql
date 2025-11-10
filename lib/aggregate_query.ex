# SPDX-FileCopyrightText: 2024 ash_sql contributors <https://github.com/ash-project/ash_sql/graphs.contributors>
#
# SPDX-License-Identifier: MIT

defmodule AshSql.AggregateQuery do
  @moduledoc false
  import Ecto.Query, only: [from: 2, subquery: 1]

  def run_aggregate_query(original_query, aggregates, resource, implementation) do
    original_query =
      AshSql.Bindings.default_bindings(original_query, resource, implementation)

    # Debug: Check aggregate structure
    Enum.each(aggregates, fn agg ->
      IO.puts("DEBUG: Aggregate name: #{agg.name}")
      IO.puts("DEBUG: Aggregate multitenancy field: #{inspect(Map.get(agg, :multitenancy))}")
      IO.puts("DEBUG: Aggregate context: #{inspect(agg.context[:shared])}")
    end)

    # Check if any aggregate has bypass multitenancy with context strategy
    bypass_context_multitenancy? =
      Enum.any?(aggregates, fn agg ->
        # Check both direct multitenancy field and context
        has_bypass =
          Map.get(agg, :multitenancy) == :bypass ||
          agg.context[:shared][:multitenancy] == :bypass_all

        is_context =
          case agg.relationship_path do
            [] ->
              Ash.Resource.Info.multitenancy_strategy(resource) == :context
            path ->
              related = Ash.Resource.Info.related(resource, path)
              Ash.Resource.Info.multitenancy_strategy(related) == :context
          end

        IO.puts("DEBUG: Aggregate #{agg.name} - has_bypass: #{has_bypass}, is_context: #{is_context}")
        has_bypass && is_context
      end)

    if bypass_context_multitenancy? do
      IO.puts("DEBUG: Running bypass context aggregate query")
      run_bypass_context_aggregate_query(original_query, aggregates, resource, implementation)
    else
      IO.puts("DEBUG: Running normal aggregate query")
      run_normal_aggregate_query(original_query, aggregates, resource, implementation)
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

  # Special handling for bypass aggregates with context multitenancy
  defp run_bypass_context_aggregate_query(original_query, aggregates, resource, implementation) do
    repo = AshSql.dynamic_repo(resource, implementation, original_query)

    # Get all tenants
    all_tenants =
      if function_exported?(repo, :all_tenants, 0) do
        repo.all_tenants()
      else
        []
      end

    # If no tenants, return default values
    if all_tenants == [] do
      {:ok, build_default_aggregate_results(aggregates)}
    else
      # Build results for each aggregate
      result =
        Enum.reduce(aggregates, %{}, fn agg, acc ->
          value =
            if Map.get(agg, :multitenancy) == :bypass do
              # Query across all tenants for bypass aggregates
              query_all_tenants_aggregate(agg, resource, all_tenants, repo, implementation)
            else
              # Query single tenant for normal aggregates
              query_single_tenant_aggregate(agg, resource, original_query, repo, implementation)
            end

          Map.put(acc, agg.name, value)
        end)

      {:ok, result}
    end
  end

  defp build_default_aggregate_results(aggregates) do
    Enum.reduce(aggregates, %{}, fn agg, acc ->
      default_value =
        case agg.kind do
          :count -> 0
          :exists -> false
          :list -> []
          _ -> nil
        end

      Map.put(acc, agg.name, default_value)
    end)
  end

  defp query_all_tenants_aggregate(agg, resource, all_tenants, repo, implementation) do
    # Get the relationship path
    related_resource =
      case agg.relationship_path do
        [] -> resource
        path -> Ash.Resource.Info.related(resource, path)
      end

    table_name = implementation.table(related_resource)

    # Build UNION ALL query across all tenants
    union_query =
      all_tenants
      |> Enum.map(fn tenant ->
        case agg.kind do
          :count ->
            "SELECT COUNT(*) as value FROM \"#{tenant}\".\"#{table_name}\""

          :exists ->
            "SELECT EXISTS(SELECT 1 FROM \"#{tenant}\".\"#{table_name}\" LIMIT 1) as value"

          :list ->
            field = to_string(agg.field || :id)
            "SELECT \"#{field}\" as value FROM \"#{tenant}\".\"#{table_name}\""

          :sum ->
            field = to_string(agg.field)
            "SELECT \"#{field}\" as value FROM \"#{tenant}\".\"#{table_name}\" WHERE \"#{field}\" IS NOT NULL"

          :max ->
            field = to_string(agg.field)
            "SELECT \"#{field}\" as value FROM \"#{tenant}\".\"#{table_name}\" WHERE \"#{field}\" IS NOT NULL"

          :min ->
            field = to_string(agg.field)
            "SELECT \"#{field}\" as value FROM \"#{tenant}\".\"#{table_name}\" WHERE \"#{field}\" IS NOT NULL"

          :avg ->
            field = to_string(agg.field)
            "SELECT \"#{field}\" as value FROM \"#{tenant}\".\"#{table_name}\" WHERE \"#{field}\" IS NOT NULL"

          :first ->
            field = to_string(agg.field || :id)
            "SELECT \"#{field}\" as value FROM \"#{tenant}\".\"#{table_name}\" LIMIT 1"

          _ ->
            "SELECT COUNT(*) as value FROM \"#{tenant}\".\"#{table_name}\""
        end
      end)
      |> Enum.join(" UNION ALL ")

    # Wrap union query with aggregation
    final_query =
      case agg.kind do
        :count ->
          "SELECT COALESCE(SUM(value), 0) FROM (#{union_query}) as combined"

        :exists ->
          "SELECT BOOL_OR(value) FROM (#{union_query}) as combined"

        :list ->
          "SELECT ARRAY_AGG(DISTINCT value) FROM (#{union_query}) as combined WHERE value IS NOT NULL"

        :sum ->
          "SELECT SUM(value) FROM (#{union_query}) as combined"

        :max ->
          "SELECT MAX(value) FROM (#{union_query}) as combined"

        :min ->
          "SELECT MIN(value) FROM (#{union_query}) as combined"

        :avg ->
          "SELECT AVG(value) FROM (#{union_query}) as combined"

        :first ->
          "SELECT value FROM (#{union_query} LIMIT 1) as combined"

        _ ->
          "SELECT COALESCE(SUM(value), 0) FROM (#{union_query}) as combined"
      end

    result = repo.query!(final_query)

    case result.rows do
      [[nil]] ->
        # Return appropriate default for nil results
        case agg.kind do
          :count -> 0
          :exists -> false
          :list -> []
          _ -> nil
        end
      [[value]] -> value
      _ ->
        case agg.kind do
          :count -> 0
          :exists -> false
          :list -> []
          _ -> nil
        end
    end
  end

  defp query_single_tenant_aggregate(agg, resource, original_query, repo, implementation) do
    # Use the existing single aggregate logic for non-bypass aggregates
    {:ok, result} =
      run_normal_aggregate_query(original_query, [agg], resource, implementation)

    Map.get(result, agg.name)
  end

  def add_single_aggs(result, resource, query, cant_group, implementation) do
    Enum.reduce(cant_group, result, fn
      %{kind: :exists} = agg, result ->
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
end
