-- ===== Contagem de Comida — Mercearia Amauri =====
-- Roda no MESMO projeto Supabase do app de bebidas, mas numa tabela separada (docs_cozinha).
-- Nada aqui mexe na tabela "docs" das bebidas.
-- Cole no Supabase em: SQL Editor > New query > Run. Depois rode o seed.sql.

create table if not exists docs_cozinha (
  path text primary key,
  col text not null,
  data jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now()
);
create index if not exists docs_cozinha_col_idx on docs_cozinha (col);

-- Função de junção campo a campo (cópia própria, para não mexer na do app de bebidas).
create or replace function jsonb_merge_deep_cozinha(a jsonb, b jsonb)
returns jsonb language sql immutable as $$
  select case
    when jsonb_typeof(a) <> 'object' or jsonb_typeof(b) <> 'object' then b
    else (
      select coalesce(jsonb_object_agg(k, v), '{}'::jsonb) from (
        select k,
          case
            when a ? k and b ? k then jsonb_merge_deep_cozinha(a->k, b->k)
            when b ? k then b->k
            else a->k
          end as v
        from (select jsonb_object_keys(a) as k union select jsonb_object_keys(b)) ks
      ) m
    )
  end
$$;

-- Grava juntando com o que já existe: duas pessoas lançando itens diferentes
-- do mesmo ambiente ao mesmo tempo não apagam o trabalho uma da outra.
create or replace function doc_merge_cozinha(p_path text, p_col text, p_patch jsonb)
returns void language plpgsql as $$
begin
  insert into docs_cozinha (path, col, data, updated_at)
  values (p_path, p_col, p_patch, now())
  on conflict (path) do update
    set data = jsonb_merge_deep_cozinha(docs_cozinha.data, excluded.data),
        updated_at = now();
end $$;

-- Atualização em tempo real entre os celulares
do $$
begin
  begin
    alter publication supabase_realtime add table docs_cozinha;
  exception when duplicate_object then null;
  end;
end $$;
alter table docs_cozinha replica identity full;

-- Acesso: igual ao app de bebidas (chave pública + PIN dentro do app).
alter table docs_cozinha enable row level security;
drop policy if exists app_all_cozinha on docs_cozinha;
create policy app_all_cozinha on docs_cozinha for all to anon using (true) with check (true);

-- Consultas prontas para relatório no próprio Supabase
create or replace view v_cozinha_produtos with (security_invoker = true) as
  select path, data->>'codigo' as codigo, data->>'descricao' as descricao, data->>'categoria' as categoria,
         data->>'unidade' as unidade, data->>'tipo' as tipo, data->>'forma' as forma, (data->>'ativo')::boolean as ativo, data
  from docs_cozinha where col = 'produtos';
create or replace view v_cozinha_contagens with (security_invoker = true) as
  select path, data->>'data' as data_contagem, data->>'tipo' as tipo, data->>'par' as domingo_conferido,
         data->>'responsavel' as responsavel, data->>'status' as status, data
  from docs_cozinha where col = 'contagens';
