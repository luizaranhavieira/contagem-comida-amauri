/* ===== Banco: adaptador Supabase com a mesma interface usada pelo app ===== */
const CFG = {
  get url() { try { return localStorage.getItem('amauri-cozinha:supaUrl') || (window.SUPABASE_URL || ''); } catch { return window.SUPABASE_URL || ''; } },
  get key() { try { return localStorage.getItem('amauri-cozinha:supaKey') || (window.SUPABASE_ANON_KEY || ''); } catch { return window.SUPABASE_ANON_KEY || ''; } },
  set(url, key) { try { localStorage.setItem('amauri-cozinha:supaUrl', url); localStorage.setItem('amauri-cozinha:supaKey', key); } catch {} },
  limpar() { try { localStorage.removeItem('amauri-cozinha:supaUrl'); localStorage.removeItem('amauri-cozinha:supaKey'); } catch {} },
};
const TABELA = 'docs_cozinha';
const erroDb = (e) => ({ code: (e && e.code) || 'unavailable', message: (e && e.message) || 'Falha na conexão com o banco' });
const colDe = (path) => path.split('/').slice(0, -1).join('/');
const idNovo = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 7);

function criarDB(url, key) {
  const sb = window.supabase.createClient(url, key, { auth: { persistSession: false }, realtime: { params: { eventsPerSecond: 4 } } });
  let canal = 0;

  function doc(path) {
    return {
      id: path.split('/').pop(), path,
      async get() {
        const { data, error } = await sb.from(TABELA).select('data').eq('path', path).maybeSingle();
        if (error) throw erroDb(error);
        return { id: path.split('/').pop(), exists: !!data, data: () => (data ? data.data : undefined), metadata: {} };
      },
      async set(d) {
        const { error } = await sb.from(TABELA).upsert({ path, col: colDe(path), data: d, updated_at: new Date().toISOString() });
        if (error) throw erroDb(error);
      },
      async update(patch) {
        const { error } = await sb.rpc('doc_merge_cozinha', { p_path: path, p_col: colDe(path), p_patch: patch });
        if (error) throw erroDb(error);
      },
      async delete() {
        const { error } = await sb.from(TABELA).delete().eq('path', path);
        if (error) throw erroDb(error);
      },
      onSnapshot(next, erro) {
        let vivo = true;
        const entregar = (d) => vivo && next({ id: path.split('/').pop(), exists: !!d, data: () => d, metadata: {} });
        sb.from(TABELA).select('data').eq('path', path).maybeSingle()
          .then(({ data, error }) => { if (error) { erro && erro(erroDb(error)); return; } entregar(data ? data.data : undefined); });
        const ch = sb.channel('d' + (++canal))
          .on('postgres_changes', { event: '*', schema: 'public', table: TABELA, filter: `path=eq.${path}` },
            (p) => entregar(p.eventType === 'DELETE' ? undefined : (p.new && p.new.data)))
          .subscribe();
        return () => { vivo = false; try { sb.removeChannel(ch); } catch {} };
      },
      collection(sub) { return colecao(path + '/' + sub); },
    };
  }

  function colecao(cpath) {
    return {
      path: cpath,
      doc: (id) => doc(cpath + '/' + (id || idNovo())),
      async get() {
        const { data, error } = await sb.from(TABELA).select('path,data').eq('col', cpath);
        if (error) throw erroDb(error);
        return montar(data || []);
      },
      onSnapshot(next, erro) {
        let vivo = true;
        const mapa = new Map();
        const emitir = () => vivo && next(montar([...mapa.entries()].map(([path, d]) => ({ path, data: d }))));
        sb.from(TABELA).select('path,data').eq('col', cpath).then(({ data, error }) => {
          if (error) { erro && erro(erroDb(error)); return; }
          (data || []).forEach((r) => mapa.set(r.path, r.data));
          emitir();
        });
        const ch = sb.channel('c' + (++canal))
          .on('postgres_changes', { event: '*', schema: 'public', table: TABELA, filter: `col=eq.${cpath}` }, (p) => {
            if (p.eventType === 'DELETE') mapa.delete(p.old && p.old.path);
            else mapa.set(p.new.path, p.new.data);
            emitir();
          })
          .subscribe();
        return () => { vivo = false; try { sb.removeChannel(ch); } catch {} };
      },
    };
  }
  function montar(linhas) {
    const docs = linhas.filter((r) => r.data).sort((a, b) => (a.path < b.path ? -1 : 1))
      .map((r) => ({ id: r.path.split('/').pop(), exists: true, data: () => r.data, metadata: {} }));
    return { docs, size: docs.length, empty: !docs.length, docChanges: () => [], metadata: {} };
  }
  return { doc, collection: colecao, _sb: sb };
}

/* Download de arquivo gerado pelo próprio app */
async function baixarArquivo(nome, blob) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = nome; a.rel = 'noopener';
  document.body.appendChild(a); a.click();
  setTimeout(() => { URL.revokeObjectURL(url); a.remove(); }, 4000);
  return { status: 'saved' };
}

/* ===== Cálculos da contagem (puro, sem DOM) ===== */
const Calc = (() => {
  const isNum = (v) => typeof v === 'number' && isFinite(v);

  /* Aceita vírgula decimal. thousandsDot: "1.450" => 1450 (pesos/volumes em g/ml). */
  function parseNum(raw, opts = {}) {
    const { thousandsDot = false, integer = false } = opts;
    if (raw === null || raw === undefined) return { empty: true, ok: true, value: null };
    let s = String(raw).trim().replace(/\s/g, '');
    if (s === '') return { empty: true, ok: true, value: null };
    if (/^-/.test(s)) return { ok: false, error: 'Valor negativo não é permitido.' };
    if (s.includes(',')) {
      s = s.replace(/\./g, '').replace(',', '.');
    } else if (thousandsDot && /^\d{1,3}(\.\d{3})+$/.test(s)) {
      s = s.replace(/\./g, '');
    }
    if (!/^\d*\.?\d+$|^\d+\.?$/.test(s)) return { ok: false, error: 'Número inválido.' };
    const v = Number(s);
    if (!isFinite(v)) return { ok: false, error: 'Número inválido.' };
    if (v < 0) return { ok: false, error: 'Valor negativo não é permitido.' };
    if (integer && !Number.isInteger(v)) return { ok: false, error: 'Use um número inteiro.' };
    return { ok: true, value: v };
  }

  /* Conversão peso -> volume: diferença direta (1 g de bebida = 1 ml) */
  function conversao(p) {
    if (!p) return { ok: false, motivo: 'Sem parâmetros' };
    if (!isNum(p.taraG)) return { ok: false, motivo: 'Tara não cadastrada' };
    return { ok: true, metodo: 'direto' };
  }

  /* Uma embalagem aberta. g = {b: bruto g, t: tara usada g|null} ; p = parâmetros da variante */
  function aberta(g, p, unidade) {
    const r = { brutoG: g.b, taraG: null, liquidoG: null, volumeMl: null, fracao: null, kg: null,
      equivUnid: null, erro: null, pendencias: [], alertas: [] };
    const tara = isNum(g.t) ? g.t : (isNum(p.taraG) ? p.taraG : null);
    r.taraG = tara;
    if (!isNum(g.b)) { r.erro = 'Peso bruto não informado.'; return r; }
    if (g.b < 0) { r.erro = 'Peso negativo não é permitido.'; return r; }
    if (tara === null) { r.pendencias.push('Recipiente não escolhido — kg da pesagem pendente'); return r; }
    if (g.b < tara) {
      r.erro = `Peso da balança (${fmtAuto(g.b / 1000, 3)} kg) menor que a tara do recipiente (${fmt(tara)} g). Confira a pesagem ou o recipiente.`;
      return r;
    }
    r.liquidoG = g.b - tara;
    if (unidade === 'KG') {
      r.kg = r.liquidoG / 1000;
      if (isNum(p.pesoLiqG) && p.pesoLiqG > 0 && r.liquidoG > p.pesoLiqG)
        r.alertas.push(`Pesagem de ${fmtAuto(r.liquidoG / 1000, 3)} kg acima do peso de uma embalagem/porção fechada (${fmt(p.pesoLiqG)} g). Se for mais de uma, conte como fechadas.`);
      return r;
    }
    // volume restante: com a garrafa cheia de referência, proporção (aberta − vazia) ÷ (cheia − vazia) × volume;
    // sem ela, diferença direta (1 g de bebida = 1 ml)
    const cheia = isNum(p.pesoCheioG) && isNum(p.volumeMl) && p.volumeMl > 0 && p.pesoCheioG > tara ? p.pesoCheioG : null;
    r.pesoCheioG = cheia;
    r.volumeMl = cheia ? (r.liquidoG / (cheia - tara)) * p.volumeMl : r.liquidoG;
    if (isNum(p.volumeMl) && p.volumeMl > 0) {
      r.fracao = r.volumeMl / p.volumeMl;
      if (r.volumeMl > p.volumeMl + 1e-9)
        r.alertas.push(`Volume na garrafa (${fmt(r.volumeMl)} ml) acima da capacidade cadastrada (${fmt(p.volumeMl)} ml).`);
      if (unidade === 'UNID') r.equivUnid = r.volumeMl / p.volumeMl;
    }
    return r;
  }

  /*
   * Um item (produto/variante) em um ambiente.
   * e = lançamento salvo {c:true, s:bool, f:int|null, a:[{i,b,t}], o} | null
   * p = parâmetros; unidade; forma efetiva: 'fechadas' | 'fechadas_abertas' | 'peso'
   */
  function item(e, p, unidade, forma) {
    const r = { status: 'nao_contado', fechadas: null, abertas: [], nAbertas: 0, liqG: null, liqOk: true,
      volMl: null, volOk: true, kgAbertas: null, fechadasL: null, fechadasKg: null, equivUnid: null,
      total: null, totalOk: false, totalRotulo: unidade, pendencias: [], alertas: [], erros: [] };
    if (!e || !e.c) { r.pendencias.push('Não contado'); r.liqOk = r.volOk = false; return r; }
    r.status = e.s ? 'sem_estoque' : 'contado';
    const usaFechadas = forma !== 'peso';
    const usaAbertas = forma !== 'fechadas';
    r.fechadas = usaFechadas ? (e.s ? 0 : (isNum(e.f) ? e.f : null)) : null;
    const lista = (!e.s && usaAbertas && Array.isArray(e.a)) ? e.a : [];
    r.abertas = lista.map((g) => aberta(g, p, unidade));
    r.nAbertas = r.abertas.length;
    let liq = 0, vol = 0, eq = 0;
    for (const a of r.abertas) {
      if (a.erro) r.erros.push(a.erro);
      a.alertas.forEach((x) => r.alertas.push(x));
      if (a.liquidoG === null) { r.liqOk = false; r.volOk = false; continue; }
      liq += a.liquidoG;
      if (a.volumeMl === null) r.volOk = false; else vol += a.volumeMl;
      if (a.equivUnid === null) eq = null; else if (eq !== null) eq += a.equivUnid;
    }
    const pend = new Set();
    r.abertas.forEach((a) => a.pendencias.forEach((x) => pend.add(x)));
    r.liqG = r.liqOk ? liq : null;
    if (unidade === 'KG') {
      r.volOk = true; r.volMl = null;
      r.kgAbertas = r.liqOk ? liq / 1000 : null;
    } else {
      r.volMl = r.volOk ? vol : null;
    }
    if (unidade === 'UNID' && r.nAbertas > 0 && r.volOk && eq !== null) r.equivUnid = eq;

    // Totais na unidade de controle
    if (unidade === 'LITRO') {
      if (usaFechadas) {
        if (r.fechadas === 0) r.fechadasL = 0;
        else if (isNum(p.volumeMl) && p.volumeMl > 0) r.fechadasL = r.fechadas * p.volumeMl / 1000;
        else pend.add('Volume nominal não cadastrado — litros das fechadas pendentes');
      } else r.fechadasL = 0;
      if (r.fechadasL !== null && r.volOk && r.liqOk) { r.total = r.fechadasL + (r.volMl || 0) / 1000; r.totalOk = true; }
      r.totalRotulo = 'L';
    } else if (unidade === 'KG') {
      if (usaFechadas) {
        if (r.fechadas === 0) r.fechadasKg = 0;
        else if (isNum(p.pesoLiqG) && p.pesoLiqG > 0) r.fechadasKg = r.fechadas * p.pesoLiqG / 1000;
        else pend.add('Peso por embalagem/porção não cadastrado — kg das fechadas pendente');
      } else r.fechadasKg = 0;
      if (r.fechadasKg !== null && r.liqOk) { r.total = r.fechadasKg + (r.kgAbertas || 0); r.totalOk = true; }
      r.totalRotulo = 'kg';
    } else {
      // UNID: total = unidades fechadas; abertas ficam separadas
      if (usaFechadas && r.fechadas !== null) { r.total = r.fechadas; r.totalOk = true; }
      r.totalRotulo = 'un. fechadas';
    }
    if (usaFechadas && !e.s && r.fechadas === null) pend.add('Quantidade de fechadas não informada');
    r.pendencias = [...pend];
    return r;
  }

  /* Consolida resultados de vários ambientes (mesma variante) */
  function consolidar(lista, unidade) {
    const r = { fechadas: 0, nAbertas: 0, liqG: 0, volMl: 0, total: 0, totalOk: true, liqOk: true, volOk: true,
      equivUnid: 0, equivOk: true, naoContados: 0, contados: 0 };
    for (const x of lista) {
      // em branco num ambiente = não tinha lá (como na planilha de papel); o total soma os ambientes contados
      if (x.status === 'nao_contado') { r.naoContados++; continue; }
      r.contados++;
      r.fechadas += x.fechadas || 0;
      r.nAbertas += x.nAbertas;
      if (x.liqG === null) r.liqOk = false; else r.liqG += x.liqG;
      if (unidade !== 'KG') { if (x.volMl === null) r.volOk = false; else r.volMl += x.volMl; }
      if (!x.totalOk) r.totalOk = false; else r.total += x.total;
      if (x.nAbertas > 0) { if (x.equivUnid === null) r.equivOk = false; else r.equivUnid += x.equivUnid; }
    }
    if (!r.contados) { r.fechadas = null; r.liqG = null; r.volMl = null; r.totalOk = false; }
    if (!r.liqOk) r.liqG = null;
    if (!r.volOk || unidade === 'KG') r.volMl = null;
    if (!r.totalOk) r.total = null;
    if (!r.equivOk || r.nAbertas === 0) r.equivUnid = null;
    return r;
  }

  /* Campos do cadastro que faltam para calcular tudo */
  function camposPendentes(prod, v) {
    const f = [];
    const forma = prod.forma;
    const temAbertas = forma !== 'fechadas';
    if (prod.unidade === 'LITRO' && forma !== 'peso' && !isNum(v.volumeMl)) f.push('volume nominal');
    if (prod.unidade === 'KG') {
      if (forma !== 'peso' && !isNum(v.pesoLiqG)) f.push('peso por embalagem/porção');
    }
    return f;
  }

  function fmt(n, dec = 0) {
    if (!isNum(n)) return '—';
    return n.toLocaleString('pt-BR', { minimumFractionDigits: dec, maximumFractionDigits: dec });
  }
  function fmtAuto(n, maxDec = 3) {
    if (!isNum(n)) return '—';
    return n.toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: maxDec });
  }
  const round = (n, d = 3) => (isNum(n) ? Math.round(n * 10 ** d) / 10 ** d : n);

  return { parseNum, conversao, aberta, item, consolidar, camposPendentes, fmt, fmtAuto, round, isNum };
})();
if (typeof module !== 'undefined') module.exports = Calc;

/* ===== Regras de cadastro inicial a partir da planilha ===== */
const Defaults = (() => {
  const AMBIENTES = [
    { id: 'prod-fria', nome: 'Produção – Câmara Fria', ordem: 1, ativo: true },
    { id: 'prod-congelada', nome: 'Produção – Câmara Congelada', ordem: 2, ativo: true },
    { id: 'final-fria', nome: 'Finalização – Câmara Fria', ordem: 3, ativo: true },
  ];
  const MARCAS = { AM: 'Mercearia Amauri', BB: 'BamBam Café', MC: 'Mercearia Campos', NM: 'Nami Gastrobar', MG: 'Mamma Gorda', CD: 'CD' };

  function norm(s) {
    return String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toUpperCase().trim();
  }

  /* Categoria pela descrição (a ordem importa: a primeira regra que bate vence) */
  const CATS = [
    ['SALGADOS', /COXINHA|CROQUET|PASTEL DE|DO PASTEL|BOLINHO/],
    ['SOBREMESAS', /BROWNIE|CARAMELO|SORVETE|DOCE DE LEITE|CHURRO/],
    ['PÃES E MASSAS', /\bPAO\b|MASSA DE|FETTUC|RIGAT|CROUTON|ESPAGUETE|PENNE/],
    ['AVES', /FRANGO|COXA E SOBRE|SOBRECOXA|PATO\b/],
    ['PEIXES E FRUTOS DO MAR', /CAMARAO|LULA|SALMAO|SAINT PETER|BACALHAU|PEIXE|POLVO|ATUM/],
    ['SUÍNOS E EMBUTIDOS', /BACON|LINGUICA|PAIO|SUINA|SUINO|TORRESMO|PERTENCES|PANCETTA/],
    ['CARNES', /ANCHO|BRISKET|BANANINHA|CARNE|PATINHO|CARPACCIO|ALCATRA|BABY BEEF|ENTRECOTE|MIGNON|FRALDA|ISCA|KATSU|MILANESA|MINEIRINHO|PAILLARD|PICADINHO|PICANHA|STROGONOFF|TARTAR|TUTANO|CORDEIRO|HAMBURGUER|COSTELA/],
    ['MOLHOS E BASES', /AIOLI|MAIONESE|MOLHO|POMODORO|DEMIGLA|BECHAMEL|BBQ|LEITE DE TIGRE|GELEIA|MANTEIGA DE ERVAS|OLEO DE|PIMENTA DA CASA|PURE /],
    ['LATICÍNIOS E QUEIJOS', /QUEIJO|QJO|MUSSARELA|BURRATA|STRACCIATELLA|REQUEIJAO|CREME DE LEITE|MANTEIGA/],
    ['HORTIFRUTI, GRÃOS E POLPAS', /COGUMELO|MANDIOCA|BATATA|POLENTA|POLPA|ALCAP|QUINOA|FEIJAO|ARROZ|LEGUME/],
  ];
  function categoriaPara(desc) {
    const d = norm(desc);
    for (const [c, re] of CATS) if (re.test(d)) return c;
    return 'OUTROS';
  }

  /* PP = produção própria, PC = porcionado; siglas seguintes = marcas que usam */
  function tipoMarcas(desc) {
    const d = norm(desc);
    const m = d.match(/\b(PP|PC)((?:\s*-\s*(?:AM|BB|MC|NM|MG|CD)\b)+)/);
    if (!m) return { tipo: 'INSUMO', marcas: [] };
    const marcas = [...new Set((m[2].match(/AM|BB|MC|NM|MG|CD/g) || []))];
    return { tipo: m[1], marcas };
  }

  /* Pesos escritos na própria descrição: "160G", "2KG", "400G 600G"; faixa "300-600G" não vale */
  function pesosDaDescricao(desc) {
    const d = norm(desc);
    if (/\d+\s*-\s*\d+\s*G\b/.test(d)) return [];
    const out = [];
    for (const m of d.matchAll(/(\d+(?:[.,]\d+)?)\s?(KG|G)\b/g)) {
      const n = Number(m[1].replace(',', '.'));
      if (n > 0) out.push(m[2] === 'KG' ? n * 1000 : n);
    }
    return [...new Set(out)];
  }

  /* Como contar: devolve forma, embalagem, tara e variantes sugeridas (tudo editável depois) */
  function sugestao(desc, unidade) {
    const { tipo } = tipoMarcas(desc);
    const pesos = pesosDaDescricao(desc);
    if (unidade === 'UNID') return { forma: 'fechadas', embalagem: 'unidade', taraG: null, variantes: [{ nome: 'Padrão', pesoLiqG: null }] };
    if (pesos.length) {
      const emb = pesos.some((g) => g >= 1000) ? 'pacote' : 'porcao';
      return { forma: 'fechadas_abertas', embalagem: emb, taraG: 0, notaPeso: true,
        variantes: pesos.map((g) => ({ nome: emb === 'porcao' ? `Porção ${FMTG(g)}` : `Pacote ${FMTG(g)}`, pesoLiqG: g })) };
    }
    if (tipo === 'PP') return { forma: 'peso', embalagem: 'pote', taraG: null, variantes: [{ nome: 'Padrão', pesoLiqG: null }] };
    return { forma: 'peso', embalagem: tipo === 'PC' ? 'saco' : 'pacote', taraG: 0, variantes: [{ nome: 'Padrão', pesoLiqG: null }] };
  }
  const FMTG = (g) => (g >= 1000 ? `${String(g / 1000).replace('.', ',')} kg` : `${g} g`);

  /* Compatível com a versão de bebidas (importação e tela de prévia) */
  function formaPara(cat, unidade, desc) { return sugestao(desc, norm(unidade)).forma; }

  function slugCodigo(desc) {
    return 'SC-' + norm(desc).split(/\s+-|-\s+/)[0].replace(/[^A-Z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 40);
  }

  function novoProduto(row, origem) {
    const unidade = norm(row.unidade) || 'KG';
    const sg = sugestao(row.descricao, unidade);
    const tm = tipoMarcas(row.descricao);
    const amb = {};
    for (const a of AMBIENTES) amb[a.id] = { abertas: sg.forma === 'fechadas_abertas' };
    const conferir = [...(row.conferir || [])];
    const variantes = sg.variantes.map((v, ix) => ({
      id: 'v' + (ix + 1), nome: v.nome, embalagem: sg.embalagem,
      volumeMl: null, pesoLiqG: v.pesoLiqG, taraG: sg.taraG, pesoCheioG: null, densidade: null, metodo: '',
      condPesagem: '', ativo: true, notas: sg.notaPeso ? { pesoLiqG: 'Tirado do nome na planilha — conferir' } : {},
    }));
    return {
      codigo: String(row.codigo).trim(),
      descricao: String(row.descricao).trim(),
      categoria: row.categoria ? String(row.categoria).trim() : categoriaPara(row.descricao),
      marca: '',
      tipo: tm.tipo,
      marcas: tm.marcas,
      unidade,
      forma: sg.forma,
      amb,
      foto: null,
      obs: '',
      conferir,
      ativo: true,
      origem: origem || '',
      variantes,
      criadoEm: Date.now(),
      atualizadoEm: Date.now(),
    };
  }

  /* Lê as linhas (array de arrays) da planilha "CONTAGEM CÂMARAS" (Item | Descrição Item | UM | locais… | total) */
  function lerLinhas(rows) {
    let hdr = -1;
    const col = {};
    for (let i = 0; i < Math.min(rows.length, 40); i++) {
      const r = rows[i] || [];
      const idx = r.findIndex((v) => ['CODIGO', 'ITEM', 'COD', 'COD.'].includes(norm(v)));
      const idd = r.findIndex((v) => norm(v).startsWith('DESCRI'));
      if (idx >= 0 && idd >= 0) {
        hdr = i;
        r.forEach((v, j) => {
          const n = norm(v);
          if (['CODIGO', 'ITEM', 'COD', 'COD.'].includes(n)) col.codigo = j;
          else if (n.startsWith('DESCRI')) col.descricao = j;
          else if (n === 'TIPO' || n.startsWith('CATEGORIA')) col.categoria = j;
          else if (n === 'UM' || n === 'U.M.' || n.startsWith('UNID')) col.unidade = j;
          else if (n === 'TOTAL') col.total = j;
        });
        break;
      }
    }
    if (hdr < 0 || col.codigo === undefined || col.descricao === undefined)
      return { erro: 'Não encontrei a linha de cabeçalho com "Item" (ou "Código") e "Descrição".' };
    const validos = [], ignorados = [], invalidos = [], duplicados = [];
    const vistos = new Set();
    const vazio = (v) => v === null || v === undefined || String(v).trim() === '';
    for (let i = hdr + 1; i < rows.length; i++) {
      const r = rows[i] || [];
      if (r.every(vazio)) continue;
      const descricao = r[col.descricao];
      if (vazio(descricao)) { ignorados.push({ linha: i + 1, motivo: 'Linha sem descrição' }); continue; }
      let unidade = col.unidade !== undefined ? norm(r[col.unidade]) : '';
      const conferir = [];
      // título de grupo: sem unidade e sem nada na coluna de total (ex.: "BACON" acima dos tipos de bacon)
      if (!unidade && col.total !== undefined && vazio(r[col.total])) {
        ignorados.push({ linha: i + 1, descricao: String(descricao).trim(), motivo: 'Título de grupo (sem unidade) — não é produto' });
        continue;
      }
      if (!unidade) { unidade = 'KG'; conferir.push('Unidade vazia na planilha — entrou como KG'); }
      if (unidade === 'UN' || unidade === 'UND' || unidade === 'UNIDADE') unidade = 'UNID';
      if (!['UNID', 'KG', 'LITRO'].includes(unidade)) {
        invalidos.push({ linha: i + 1, descricao: String(descricao).trim(), motivo: `Unidade "${unidade}" não reconhecida (use KG ou UNID)` });
        continue;
      }
      const d = norm(descricao);
      if (unidade === 'KG' && /\bUNIDADE\b/.test(d)) conferir.push('O nome diz "unidade", mas a planilha está em KG');
      let codigo = vazio(r[col.codigo]) ? '' : String(r[col.codigo]).trim().replace(/\.0$/, '');
      if (!codigo) { codigo = slugCodigo(descricao); conferir.push('Sem código na planilha — código provisório'); }
      const item = {
        linha: i + 1, codigo, descricao: String(descricao).trim(),
        categoria: col.categoria !== undefined && !vazio(r[col.categoria]) ? String(r[col.categoria]).trim() : categoriaPara(descricao),
        unidade, conferir,
      };
      if (vistos.has(item.codigo)) { duplicados.push({ ...item, motivo: 'Código repetido na planilha — mantida a primeira linha' }); continue; }
      vistos.add(item.codigo);
      validos.push(item);
    }
    return { validos, ignorados, invalidos, duplicados, linhaCabecalho: hdr + 1 };
  }

  return { AMBIENTES, MARCAS, novoProduto, lerLinhas, formaPara, sugestao, categoriaPara, tipoMarcas, pesosDaDescricao, norm };
})();
if (typeof module !== 'undefined') module.exports = Defaults;

/* ===== Infra ===== */
const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const LS = {
  get(k, d) { try { const v = localStorage.getItem('amauri-cozinha:' + k); return v === null ? d : JSON.parse(v); } catch { return d; } },
  set(k, v) { try { localStorage.setItem('amauri-cozinha:' + k, JSON.stringify(v)); } catch {} },
  del(k) { try { localStorage.removeItem('amauri-cozinha:' + k); } catch {} },
};
const F = Calc.fmt, FA = Calc.fmtAuto, isNum = Calc.isNum;
const uid = (p = '') => p + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
const hojeISO = () => { const d = new Date(); d.setMinutes(d.getMinutes() - d.getTimezoneOffset()); return d.toISOString().slice(0, 10); };
const dataBR = (iso) => (iso ? iso.split('-').reverse().join('/') : '—');
const horaBR = (t) => (t ? new Date(t).toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }) : '');
const collator = new Intl.Collator('pt-BR', { sensitivity: 'base', numeric: true });
const normBusca = (s) => String(s || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase();

const UNID_LABEL = { UNID: 'UNID', LITRO: 'LITRO', KG: 'KG' };
const FORMA_LABEL = { fechadas: 'Só contar (unidades / porções)', fechadas_abertas: 'Fechadas + pesagem das abertas', peso: 'Só pesagem' };
const METODO_LABEL = { '': 'Embalagem cheia de referência', A: 'Densidade (antigo)', B: 'Embalagem cheia de referência' };
const EMB = {
  porcao: { s: 'porção', f: 'Porções fechadas', add: 'Pesar porção aberta / sobra', ab: 'Porções abertas (pesagem)', pl: 'porções' },
  pacote: { s: 'pacote', f: 'Pacotes fechados', add: 'Pesar pacote aberto', ab: 'Pacotes abertos (pesagem)', pl: 'pacotes' },
  saco: { s: 'saco', f: 'Sacos fechados', add: 'Adicionar pesagem', ab: 'Pesagens', pl: 'sacos' },
  pote: { s: 'pote', f: 'Potes fechados', add: 'Adicionar pesagem', ab: 'Pesagens', pl: 'potes' },
  caixa: { s: 'caixa', f: 'Caixas fechadas', add: 'Pesar caixa aberta', ab: 'Caixas abertas (pesagem)', pl: 'caixas' },
  unidade: { s: 'unidade', f: 'Unidades', add: 'Adicionar pesagem', ab: 'Pesagens', pl: 'unidades' },
  outra: { s: 'embalagem', f: 'Embalagens fechadas', add: 'Adicionar pesagem', ab: 'Pesagens', pl: 'embalagens' },
};
const embL = (e) => EMB[e] || EMB.outra;
const TIPO_LABEL = { PP: 'Produção própria', PC: 'Porcionado', INSUMO: 'Insumo comprado' };
const TIPO_CURTO = { domingo: 'Domingo', segunda: 'Segunda', extra: 'Extra' };

/* ----- pesagem em kg (a balança da cozinha mostra kg); internamente tudo em gramas ----- */
const PESO_MAX_KG = 80;
function parsePeso(txt) {
  const r = Calc.parseNum(txt, { thousandsDot: false });
  if (!r.ok || r.empty) return r;
  if (r.value > PESO_MAX_KG) return { ok: false, error: `Digite em kg (ex.: 2,350). ${FA(r.value, 3)} kg passa do limite de ${PESO_MAX_KG} kg.` };
  return { ok: true, value: Math.round(r.value * 1000 * 10) / 10 };
}
const kgTxt = (g) => (isNum(g) ? String(Calc.round(g / 1000, 3)).replace('.', ',') : '');

/* ----- recipientes (GN, potes…) com a tara de cada um ----- */
const recipientesOrd = () => [...(S.recipientes || [])].filter((r) => r.ativo !== false).sort((a, b) => collator.compare(a.nome || '', b.nome || ''));
function recipSelect(g, it, attrs) {
  const atual = isNum(g.t) ? g.t : null;
  const taraCad = isNum(it.p.taraG) ? it.p.taraG : null;
  const ops = [];
  let achou = false;
  const op = (v, l) => { const sel = atual !== null && v === atual && !achou; if (sel) achou = true; return `<option value="${v}" ${sel ? 'selected' : ''}>${esc(l)}</option>`; };
  if (taraCad !== null) ops.push(op(taraCad, taraCad === 0 ? 'Sem recipiente (0 g)' : `Recipiente do cadastro (${F(taraCad)} g)`));
  if (taraCad !== 0) ops.push(op(0, 'Sem recipiente (0 g)'));
  for (const r of recipientesOrd()) ops.push(op(r.taraG, `${r.nome} (${F(r.taraG)} g)`));
  if (atual !== null && !achou) ops.push(`<option value="${atual}" selected>Tara ${F(atual)} g</option>`);
  return `<select class="recip" ${attrs} aria-label="Recipiente">${atual === null ? '<option value="" selected>Recipiente?</option>' : ''}${ops.join('')}</select>`;
}
const STATUS_LABEL = { andamento: 'Em andamento', pausada: 'Pausada', finalizada: 'Finalizada', parcial: 'Encerrada com pendências' };
const fechadaStatus = (st) => st === 'finalizada' || st === 'parcial';

const S = {
  db: null, dl: null, assets: null, noDb: false, semConfig: false,
  produtos: new Map(), ambientes: [], contagens: [], usuarios: [], exportacoes: {}, recipientes: [],
  config: { limPct: 10, limKg: 0.5, limUn: 2 },
  loaded: { p: false, a: false, c: false, u: false },
  user: LS.get('sessao', null),
  cur: { cid: null, lanc: {}, lancOk: {}, snap: undefined, unsubs: [] },
  cache: {},
  draft: null, saving: false,
  ui: { busca: '', cat: '', filtro: 'pend', cadBusca: '', cadFiltro: 'todos', cadCat: '', resAmb: '', resModo: 'amb', expAmb: 'todos',
    hDe: '', hAte: '', hAmb: '', hResp: '', cmpSel: [], cmpAmb: 'cons', impr: null, impUpd: false, pdraft: null, novo: null },
  sync: { pend: 0, last: null, err: null },
  me: LS.get('contador', ''),
};

/* ----- toast & modal ----- */
let toastT;
function toast(msg, ms = 2200) {
  let t = $('#toast');
  if (!t) { t = document.createElement('div'); t.id = 'toast'; t.className = 'toast'; t.setAttribute('role', 'status'); document.body.appendChild(t); }
  t.textContent = msg; t.hidden = false;
  clearTimeout(toastT); toastT = setTimeout(() => { t.hidden = true; }, ms);
}
function modal({ title, body = '', actions = [{ label: 'OK', value: true, cls: 'pri' }], onOpen }) {
  return new Promise((resolve) => {
    const m = document.createElement('div');
    m.className = 'modal';
    m.innerHTML = `<div class="box" role="dialog" aria-modal="true" aria-label="${esc(title)}">
      <h2>${esc(title)}</h2><div class="stack">${body}</div>
      <div class="stack">${actions.map((a, i) => `<button class="btn big full ${a.cls || ''}" data-mi="${i}" id="mi-${i}">${esc(a.label)}</button>`).join('')}</div></div>`;
    document.body.appendChild(m);
    const close = (v) => { m.remove(); resolve(v); };
    m.addEventListener('click', (ev) => {
      const b = ev.target.closest('[data-mi]');
      if (b) {
        const a = actions[+b.dataset.mi];
        if (a.check && !a.check(m)) return;
        close(a.collect ? a.collect(m) : a.value);
      } else if (ev.target === m) close(null);
    });
    m.addEventListener('change', () => onOpen && onOpen(m, true));
    onOpen && onOpen(m);
    setTimeout(() => { const f = m.querySelector('input,textarea,select') || m.querySelector('[data-mi]'); f && f.focus(); }, 30);
  });
}
const confirmar = (title, body, label = 'Confirmar', cls = 'pri') =>
  modal({ title, body: `<p>${body}</p>`, actions: [{ label, value: true, cls }, { label: 'Cancelar', value: false, cls: 'ghost' }] });

/* ----- sync wrapper ----- */
async function comSync(fn) {
  S.sync.pend++; paintSync();
  try {
    const r = await fn();
    S.sync.last = Date.now(); S.sync.err = null;
    return r;
  } catch (e) {
    S.sync.err = (e && (e.message || e.code)) || 'Falha ao salvar';
    throw e;
  } finally { S.sync.pend--; paintSync(); }
}
function syncHTML() {
  if (S.noDb) return '<span class="sync err">Sem conexão com o banco</span>';
  if (S.sync.pend > 0) return '<span class="sync">Salvando…</span>';
  if (S.sync.err) return '<span class="sync err">Erro ao salvar</span>';
  if (S.sync.last) return `<span class="sync ok">✓ Salvo ${new Date(S.sync.last).toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })}</span>`;
  return '<span class="sync ok">Online</span>';
}
function paintSync() { $$('[data-sync]').forEach((el) => { el.innerHTML = syncHTML(); }); }
function errMsg(e) {
  const c = e && e.code;
  if (c === 'quota_exceeded') return 'Limite de armazenamento atingido. Exclua contagens antigas para liberar espaço.';
  if (c === 'invalid_argument') return 'Você não tem permissão para gravar aqui ou o dado é inválido.';
  if (c === 'resource_exhausted') return 'Muitas gravações seguidas. Aguarde alguns segundos e tente de novo.';
  return 'Não foi possível salvar. Verifique a internet e tente novamente.';
}

/* ----- routing ----- */
function route() {
  const h = decodeURIComponent(location.hash.replace(/^#\/?/, ''));
  const p = h.split('/').filter(Boolean);
  if (!p.length) return { name: 'home' };
  if (p[0] === 'nova') return { name: 'nova' };
  if (p[0] === 'c') {
    const cid = p[1];
    if (p[2] === 'a' && p[4] === 'i') return { name: 'item', cid, amb: p[3], key: p.slice(5).join('/') };
    if (p[2] === 'a') return { name: 'amb', cid, amb: p[3] };
    if (p[2] === 'revisao') return { name: 'revisao', cid };
    if (p[2] === 'resumo') return { name: 'resumo', cid };
    return { name: 'hub', cid };
  }
  if (p[0] === 'hist') return { name: 'hist' };
  if (p[0] === 'gestao') return { name: 'gestao' };
  if (p[0] === 'comparar') return { name: 'comparar', a: p[1], b: p[2] };
  if (p[0] === 'cad') {
    if (p[1] === 'p') return { name: 'produto', code: p.slice(2).join('/') };
    if (p[1] === 'a') return { name: 'ambiente', id: p[2] };
    return { name: 'cad', tab: p[1] || 'prod' };
  }
  return { name: 'home' };
}
let goBusy = false;
const go = (h) => { if (location.hash !== h) { location.hash = h; return; } if (goBusy) return; goBusy = true; try { render(); } finally { goBusy = false; } };
let R = route();
window.addEventListener('hashchange', () => {
  const prev = R; R = route();
  if (prev.name === 'item' && S.draft && S.draft.dirty && !(R.name === 'item' && R.key === prev.key)) {
    // rascunho fica guardado no aparelho
    toast('Lançamento não salvo guardado como rascunho neste aparelho');
  }
  if (R.name !== 'item') S.draft = null;
  if (R.name !== 'produto') S.ui.pdraft = null;
  ensureContagem(R.cid || null);
  render(); window.scrollTo(0, 0);
  if (R.name === 'amb' && S.ui.rolarPara) {
    const alvo = S.ui.rolarPara.map((k) => document.querySelector(`[data-key="${CSS.escape(k)}"]`)).find(Boolean);
    S.ui.rolarPara = null;
    if (alvo) alvo.scrollIntoView({ block: 'center' });
  }
});

/* ----- dados ----- */
const ambAtivos = () => S.ambientes.filter((a) => a.ativo !== false).sort((a, b) => (a.ordem || 0) - (b.ordem || 0));
const ambNome = (id, h) => (S.ambientes.find((a) => a.id === id) || {}).nome || (h && h.ambNomes && h.ambNomes[id]) || id;
const contagem = (cid) => S.contagens.find((c) => c.id === cid);
const produtosOrdenados = () => [...S.produtos.values()].sort((a, b) =>
  collator.compare(a.categoria || '', b.categoria || '') || collator.compare(a.descricao || '', b.descricao || ''));
const categorias = () => [...new Set([...S.produtos.values()].map((p) => p.categoria || 'SEM CATEGORIA'))].sort(collator.compare);
const paramsDe = (v) => ({ volumeMl: v.volumeMl ?? null, pesoLiqG: v.pesoLiqG ?? null, taraG: v.taraG ?? null,
  pesoCheioG: v.pesoCheioG ?? null, densidade: v.densidade ?? null, metodo: v.metodo || '', condPesagem: v.condPesagem || '' });
const formaEfetiva = (forma, abertasAqui) => (forma === 'fechadas_abertas' && abertasAqui === false ? 'fechadas' : forma);

function ensureContagem(cid) {
  if (S.cur.cid === cid) return;
  S.cur.unsubs.forEach((u) => { try { u(); } catch {} });
  S.cur = { cid, lanc: {}, lancOk: {}, snap: undefined, unsubs: [] };
  if (!cid || !S.db) return;
  const h = contagem(cid);
  if (!h) return; // assina quando a lista de contagens chegar
  subscribeContagem(h);
}
function assinarLanc(cid, amb) {
  const u = S.db.doc(`contagens/${cid}/lanc/${amb}`).onSnapshot((d) => {
    S.cur.lanc[amb] = d.exists ? (d.data().i || {}) : {};
    S.cur.lancOk[amb] = true;
    onData('lanc');
  }, () => {});
  S.cur.unsubs.push(u);
}
function subscribeContagem(h) {
  const cid = h.id;
  for (const amb of h.ambientes || []) assinarLanc(cid, amb);
  const u2 = S.db.doc(`contagens/${cid}/snap/params`).onSnapshot((d) => {
    S.cur.snap = d.exists ? d.data() : null;
    onData('snap');
  }, () => {});
  S.cur.unsubs.push(u2);
  S.cur.subscribed = true;
}

/* Contexto de uma contagem: itens por ambiente e parâmetros (vivos ou congelados) */
function ctxDe(h, lancMap, snap) {
  const congelado = fechadaStatus(h.status) && snap;
  const ambs = [...new Set(h.ambientes || [])].map((id) => ({ id, nome: (congelado && snap.amb[id] && snap.amb[id].nome) || ambNome(id, h) }));
  const cacheItens = {};
  function itens(ambId) {
    if (cacheItens[ambId]) return cacheItens[ambId];
    let lista = [];
    const lanc = lancMap[ambId] || {};
    if (congelado) {
      const sa = snap.amb[ambId] || { itens: [] };
      lista = sa.itens.map((x) => {
        const v = snap.v[x.k] || {};
        return { key: x.k, ...v, forma: x.forma, p: v.params || {} };
      });
    } else {
      const vistos = new Set();
      for (const prod of produtosOrdenados()) {
        const cfg = prod.amb && prod.amb[ambId];
        const ativo = prod.ativo !== false && cfg;
        for (const v of prod.variantes || []) {
          const key = `${prod.codigo}~${v.id}`;
          if (!(ativo && v.ativo !== false) && !lanc[key]) continue;
          vistos.add(key);
          lista.push(itemVivo(prod, v, cfg, key));
        }
      }
      for (const key of Object.keys(lanc)) {
        if (vistos.has(key) || !lanc[key]) continue;
        const [codigo, vid] = key.split('~');
        const prod = S.produtos.get(codigo);
        const v = prod && (prod.variantes || []).find((x) => x.id === vid);
        if (prod && v) lista.push(itemVivo(prod, v, prod.amb && prod.amb[ambId], key));
        else lista.push({ key, codigo, descricao: `(produto removido ${codigo})`, varNome: '', categoria: 'SEM CATEGORIA', unidade: 'UNID', forma: 'fechadas', embalagem: 'outra', p: {} });
      }
    }
    lista.forEach((it) => { it.r = Calc.item(lanc[it.key], it.p, it.unidade, it.forma); it.e = lanc[it.key] || null; });
    cacheItens[ambId] = lista;
    return lista;
  }
  return { h, ambs, itens, congelado: !!congelado };
}
function itemVivo(prod, v, cfg, key) {
  const nv = (prod.variantes || []).filter((x) => x.ativo !== false).length;
  return { key, codigo: prod.codigo, descricao: prod.descricao, varNome: nv > 1 || (prod.variantes || []).length > 1 ? v.nome : '',
    varNomeFull: v.nome, categoria: prod.categoria || 'SEM CATEGORIA', unidade: prod.unidade, formaProd: prod.forma,
    forma: formaEfetiva(prod.forma, cfg && cfg.abertas), embalagem: v.embalagem || 'outra', p: paramsDe(v), foto: prod.foto || null,
    fotoMini: prod.fotoMini || null, fotoUrl: prod.fotoUrl || '', obsProd: prod.obs || '' };
}
function ctxAtual() {
  const h = contagem(S.cur.cid);
  if (!h) return null;
  return ctxDe(h, S.cur.lanc, S.cur.snap);
}
function progresso(lista) {
  const total = lista.length, contados = lista.filter((x) => x.r.status !== 'nao_contado').length;
  const conv = lista.filter((x) => x.r.status !== 'nao_contado' && x.r.pendencias.length).length;
  return { total, contados, conv, pct: total ? Math.round((contados / total) * 100) : 0 };
}

/* ----- ícones ----- */
const ICON = {
  garrafa: '<svg width="20" height="18" viewBox="0 0 24 22" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round" stroke-linecap="round"><path d="M3 20h18l-2-10H5L3 20Z"/><circle cx="12" cy="15" r="2.6"/><path d="M12 15l1.4-1.6"/><path d="M8 10V6h8v4"/><path d="M5 6h14"/></svg>',
  mais: '<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></svg>',
  search: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/></svg>',
};

function topbar({ back, eyebrow, title, extra = '' }) {
  return `<header class="top"><div class="ctx">
    ${back ? `<button class="back" data-act="back" data-to="${esc(back)}" aria-label="Voltar">‹</button>` : ''}
    <div class="t">${eyebrow ? `<div class="amb">${eyebrow}</div>` : ''}<div class="ttl">${title}</div></div>
    <div data-sync>${syncHTML()}</div></div>${extra}</header>`;
}

/* ----- render principal ----- */
function render() {
  const app = $('#app');
  const foco = document.activeElement && document.activeElement.id;
  const sel = foco && document.activeElement.selectionStart;
  let html = '';
  try {
    if (S.semConfig) html = viewConfig();
    else if (S.noDb) html = viewSemDb();
    else if (!S.loaded.p || !S.loaded.a || !S.loaded.c || !S.loaded.u) html = `<div class="loading"><div><img src="${LOGO}" alt="Mercearia Amauri"><p class="eyebrow" style="margin-top:14px">Carregando contagem…</p></div></div>`;
    else if (!S.usuarios.length) html = VIEWS.primeiroAcesso();
    else if (!sessaoValida()) html = VIEWS.login();
    else if (['cad', 'produto', 'ambiente', 'gestao', 'hist', 'revisao', 'resumo', 'item', 'comparar'].includes(R.name) && !ehGestor()) html = semPermissao(R.name === 'nova' ? 'Iniciar contagem' : 'Área do gestor');
    else html = VIEWS[R.name] ? VIEWS[R.name](R) : VIEWS.home(R);
  } catch (e) {
    console.error(e);
    html = `${topbar({ back: '#/', title: 'Erro' })}<div class="note n-err" style="margin-top:14px">Erro ao montar a tela: ${esc(e.message)}</div>`;
  }
  app.innerHTML = html;
  if (foco) {
    const el = document.getElementById(foco);
    if (el && el !== document.activeElement) { el.focus(); try { if (sel !== null && sel !== undefined) el.setSelectionRange(sel, sel); } catch {} }
  }
  AFTER.forEach((f) => f()); AFTER.length = 0;
  if (typeof focarProximo === 'function' && S.ui.focarProx) focarProximo();
}
const AFTER = [];
function onData(kind) {
  if (R.name === 'item') { atualizarItemRemoto(); return; }
  if (R.name === 'produto' && S.ui.pdraft) return;
  if (R.name === 'cad' && R.tab === 'imp' && S.ui.impr) return;
  render();
}
function viewSemDb() {
  return `<div class="hero-brand"><img class="logo" src="${LOGO}" alt="Mercearia Amauri"><h1>Contagem de comida</h1></div>
  <div class="stack" style="margin-top:16px">
  <div class="note n-err">Não consegui falar com o banco de dados. Confira a internet do aparelho e as chaves do Supabase.</div>
  <button class="btn full" data-act="abrirConfig">Rever as chaves do banco</button></div>`;
}
function viewConfig() {
  return `<div class="hero-brand"><img class="logo" src="${LOGO}" alt="Mercearia Amauri"><div class="deco">Mercearia Amauri</div><h1>Configuração</h1></div>
  <div class="stack" style="margin-top:16px">
    <div class="note n-info small">Cole aqui os dados do projeto no Supabase (Project Settings › API). Eles ficam guardados neste aparelho.</div>
    <label class="f">Project URL<input id="cfg-url" value="${esc(CFG.url)}" placeholder="https://xxxx.supabase.co" autocomplete="off"></label>
    <label class="f">Chave anon public<input id="cfg-key" value="${esc(CFG.key)}" placeholder="eyJhbGciOi..." autocomplete="off"></label>
    <button class="btn big pri full" data-act="salvarConfig">Salvar e abrir o app</button>
  </div>`;
}

/* ----- eventos (delegação) ----- */
const ACT = {};
const INP = {};
document.addEventListener('click', (ev) => {
  const el = ev.target.closest('[data-act]');
  if (!el || el.disabled) return;
  const f = ACT[el.dataset.act];
  if (f) { ev.preventDefault(); f(el, ev); }
});
document.addEventListener('input', (ev) => {
  const el = ev.target.closest('[data-in]');
  if (el && INP[el.dataset.in]) INP[el.dataset.in](el, ev);
});
document.addEventListener('change', (ev) => {
  const el = ev.target.closest('[data-ch]');
  if (el && INP[el.dataset.ch]) INP[el.dataset.ch](el, ev);
});
ACT.back = (el) => { go(el.dataset.to || '#/'); };
ACT.go = (el) => go(el.dataset.to);

/* ----- bibliotecas pesadas: só carrega quando precisa (Excel) ----- */
const LIBS = {
  ExcelJS: 'https://cdnjs.cloudflare.com/ajax/libs/exceljs/4.4.0/exceljs.min.js',
  XLSX: 'https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js',
};
const libPend = {};
function carregarLib(nome) {
  if (window[nome]) return Promise.resolve(window[nome]);
  if (libPend[nome]) return libPend[nome];
  libPend[nome] = new Promise((ok, err) => {
    const s = document.createElement('script');
    s.src = LIBS[nome]; s.async = true;
    s.onload = () => (window[nome] ? ok(window[nome]) : err(new Error('lib vazia')));
    s.onerror = () => { libPend[nome] = null; err(new Error('sem internet para baixar a biblioteca')); };
    document.head.appendChild(s);
  });
  return libPend[nome];
}

/* ----- boot ----- */
async function boot() {
  render();
  if (!CFG.url || !CFG.key) { S.semConfig = true; render(); return; }
  S.dl = { save: ({ filename, data }) => baixarArquivo(filename, data) };
  S.assets = null;
  let db;
  try { db = criarDB(CFG.url, CFG.key); } catch (e) { console.error(e); S.noDb = true; render(); return; }
  S.db = db;
  db.collection('produtos').onSnapshot((snap) => {
    S.produtos = new Map(snap.docs.map((d) => [d.id, d.data()]));
    S.loaded.p = true; onData('p');
  }, (e) => { console.error(e); S.noDb = true; render(); });
  db.collection('ambientes').onSnapshot((snap) => {
    S.ambientes = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    S.loaded.a = true; onData('a');
  }, (e) => console.error(e));
  db.collection('exportacoes').onSnapshot((snap) => {
    S.exportacoes = Object.fromEntries(snap.docs.map((d) => [d.id, d.data()]));
    if (R.name === 'resumo') onData('e');
  }, () => {});
  db.collection('recipientes').onSnapshot((snap) => {
    S.recipientes = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    onData('r');
  }, () => {});
  db.collection('config').onSnapshot((snap) => {
    const g = snap.docs.find((d) => d.id === 'geral');
    if (g) S.config = { ...S.config, ...g.data() };
    onData('cfg');
  }, () => {});
  db.collection('usuarios').onSnapshot((snap) => {
    S.usuarios = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    S.loaded.u = true; onData('u');
  }, (e) => console.error(e));
  db.collection('contagens').onSnapshot((snap) => {
    S.contagens = snap.docs.map((d) => ({ id: d.id, ...d.data() }));
    S.loaded.c = true;
    if (S.cur.cid && !S.cur.subscribed && contagem(S.cur.cid)) subscribeContagem(contagem(S.cur.cid));
    onData('c');
  }, (e) => console.error(e));
  ensureContagem(R.cid || null);
}

/* ===== Domingo + segunda ===== */
const diaSemana = (iso) => new Date(iso + 'T12:00:00').getDay();
const tipoPorData = (iso) => { const d = diaSemana(iso); return d === 0 ? 'domingo' : d === 1 ? 'segunda' : 'extra'; };
const rotuloC = (c) => (c ? `${c.tipo && c.tipo !== 'extra' ? TIPO_CURTO[c.tipo] + ' ' : ''}${dataBR(c.data)}` : '');
const diaAntes = (iso) => { const d = new Date(iso + 'T12:00:00'); d.setDate(d.getDate() - 1); return d.toISOString().slice(0, 10); };
/* contagens de domingo que podem ser conferidas numa segunda (a do dia anterior primeiro) */
function domingosCandidatos(isoSeg) {
  const ontem = diaAntes(isoSeg);
  return S.contagens.filter((c) => c.tipo === 'domingo' && fechadaStatus(c.status) && (c.data || '') <= isoSeg)
    .sort((a, b) => (b.data === ontem) - (a.data === ontem) || (b.data || '').localeCompare(a.data || '') || (b.criadoEm || 0) - (a.criadoEm || 0));
}
const domingoPara = (isoSeg) => domingosCandidatos(isoSeg)[0] || null;
const segundaDe = (domId) => S.contagens.filter((c) => c.par === domId).sort((a, b) => (b.criadoEm || 0) - (a.criadoEm || 0))[0] || null;

/* Divergência entre domingo (a) e segunda (b), pelos limites do gestor */
function divergencia(a, b, unidade) {
  if (!isNum(a) || !isNum(b)) return null;
  const diff = b - a;
  const ad = Math.abs(diff);
  if (ad < 1e-9) return { diff, pct: 0, div: false };
  const pct = a > 0 ? (ad / a) * 100 : 100;
  const c = S.config || {};
  const div = unidade === 'UNID' ? ad >= (c.limUn ?? 2) || pct >= (c.limPct ?? 10)
    : ad >= (c.limKg ?? 0.5) || pct >= (c.limPct ?? 10);
  return { diff, pct, div };
}

/* Contexto do domingo pareado com a segunda aberta (carrega uma vez) */
function ctxParDe(h) {
  if (!h || !h.par) return null;
  const hp = contagem(h.par);
  if (!hp) return null;
  const c = S.cache[h.par];
  if (!c) {
    if (!S.ui.carregandoPar) {
      S.ui.carregandoPar = true;
      carregarContagem(h.par).then(() => { S.ui.carregandoPar = false; render(); }).catch(() => { S.ui.carregandoPar = false; });
    }
    return null;
  }
  return ctxDe(hp, c.lanc, c.snap);
}
function valorPar(ctxP, ambId, key) {
  if (!ctxP || !ctxP.ambs.some((a) => a.id === ambId)) return { estado: 'ausente' };
  return valorComparado(ctxP, [ambId], key);
}
const qtdTxt = (v, unidade) => (unidade === 'UNID' ? `${F(v)} un.` : `${FA(v, 3)} ${unidade === 'KG' ? 'kg' : 'L'}`);
function linhaPar(it, ctxP, ambId) {
  if (!ctxP || it.r.status === 'nao_contado') return '';
  const vp = valorPar(ctxP, ambId, it.key);
  const atual = it.r.totalOk ? it.r.total : null;
  if (vp.estado !== 'ok') return `<br><span class="meta">domingo: ${vp.estado === 'nao_contado' ? 'não contado' : vp.estado === 'ausente' ? '—' : 'pendente'}</span>`;
  const dv = divergencia(vp.v, atual, it.unidade);
  if (!dv) return `<br><span class="meta">domingo: ${qtdTxt(vp.v, it.unidade)}</span>`;
  const cor = dv.div ? 'var(--danger)' : 'var(--ok)';
  return `<br><span class="meta">domingo: ${qtdTxt(vp.v, it.unidade)} · <b style="color:${cor}">${dv.diff > 0 ? '+' : ''}${it.unidade === 'UNID' ? F(dv.diff) : FA(dv.diff, 3)}${dv.div ? ' ⚠' : ' ✓'}</b></span>`;
}

/* ===== Telas de contagem ===== */
const VIEWS = {};

VIEWS.home = () => {
  const atual = contagemAberta();
  const semProd = S.produtos.size === 0;
  let corpo;
  if (atual && atual.status === 'pausada') corpo = `<div class="small" style="opacity:.85;margin-top:16px">Contagem de ${rotuloC(atual)} pausada.</div>
    ${ehGestor() ? `<button class="btn big full cta" data-act="retomar" data-cid="${atual.id}">Retomar contagem</button>` : '<div class="small" style="opacity:.85">O gestor retoma a contagem.</div>'}`;
  else if (atual) corpo = `<button class="btn big full cta" data-act="go" data-to="#/c/${atual.id}">Contar</button>
    <div class="small" style="margin-top:10px;opacity:.8">Contagem de ${rotuloC(atual)}</div>`;
  else {
    const t = tipoPorData(hojeISO());
    const dica = t === 'domingo' ? 'Hoje é domingo: contagem principal da semana.'
      : t === 'segunda' ? (domingoPara(hojeISO()) ? `Hoje é segunda: conferência da contagem de ${rotuloC(domingoPara(hojeISO()))}.` : 'Hoje é segunda: conferência do domingo.')
      : 'As contagens são aos domingos e conferidas na segunda.';
    corpo = `<button class="btn big full cta" data-act="go" data-to="#/nova" ${semProd ? 'disabled' : ''}>Iniciar contagem</button>
    <div class="small" style="margin-top:10px;opacity:.8">${esc(dica)}</div>`;
  }
  return `<div class="hero-brand hero-min">
    ${ehGestor() ? `<button class="gear" data-act="go" data-to="#/gestao" aria-label="Área do gestor" title="Área do gestor">⚙</button>` : ''}
    <img class="logo" src="${LOGO}" alt="Mercearia Amauri">
    <h1>Contagem de comida</h1>
    <div class="deco" style="margin-top:-4px">Câmaras · domingo e segunda</div>
    ${corpo}
  </div>
  ${S.user ? `<div class="userline"><span>${esc(S.user.nome)}</span>·<button data-act="sair">Sair</button></div>` : ''}`;
};
ACT.retomar = async (el) => {
  const h = contagem(el.dataset.cid); if (!h) return;
  try {
    await comSync(() => S.db.doc(`contagens/${h.id}`).update({ status: 'andamento', atualizadoEm: Date.now(),
      eventos: [...(h.eventos || []), { t: Date.now(), p: S.me || '', d: 'Contagem retomada' }].slice(-50) }));
    go(`#/c/${h.id}`);
  } catch (e) { toast(errMsg(e), 4000); }
};

/* ---- Área do gestor: tudo que não é contar ---- */
VIEWS.gestao = () => {
  const ab = contagemAberta();
  const abertas = S.contagens.filter((c) => !fechadaStatus(c.status));
  return `${topbar({ back: '#/', eyebrow: 'Mercearia Amauri', title: 'Área do gestor' })}
  <div class="stack" style="margin-top:16px">
    ${abertas.length > 1 ? `<div class="note n-warn small">Há ${abertas.length} contagens abertas. Só pode existir uma: exclua as outras pelo Histórico.</div>` : ''}
    ${ab ? `<section class="card stack">
      <div class="row between"><h3>Contagem de ${rotuloC(ab)}</h3><span class="pill ${ab.status === 'pausada' ? 'p-pend' : 'p-acc'}">${STATUS_LABEL[ab.status]}</span></div>
      <div class="muted small">Responsável: ${esc(ab.responsavel)} · ${(ab.ambientes || []).map((a) => esc(ambNome(a, ab))).join(', ')}</div>
      <div class="mgrid">
        <button class="btn full" data-act="go" data-to="#/c/${ab.id}/revisao">Revisar pendências</button>
        <button class="btn full" data-act="go" data-to="#/c/${ab.id}/resumo">Prévia do resumo</button>
        ${ambAtivos().some((a) => !(ab.ambientes || []).includes(a.id)) ? `<button class="btn full" data-act="incluirAmbG" data-cid="${ab.id}">+ Incluir ambiente</button>` : ''}
        ${(ab.ambientes || []).length > 1 ? `<button class="btn full" data-act="tirarAmbMenu" data-cid="${ab.id}">Tirar um ambiente</button>` : ''}
        ${ab.status !== 'pausada' ? `<button class="btn full" data-act="pausarG" data-cid="${ab.id}">Pausar contagem</button>` : ''}
        <button class="btn dan ghost full" data-act="excluirContagem" data-cid="${ab.id}">Excluir contagem</button>
      </div>
    </section>` : ''}
    <button class="card tap lift" data-act="go" data-to="#/hist"><h3>Histórico</h3><div class="muted small">Contagens anteriores, resumo, Excel e comparação</div></button>
    <button class="card tap lift" data-act="go" data-to="#/cad"><h3>Cadastro</h3><div class="muted small">Produtos, ambientes, recipientes, limites, equipe e importação</div></button>
  </div>`;
};
async function comContagem(cid, fn) {
  const volta = S.cur.cid;
  ensureContagem(cid);
  try { await fn(); } finally { if (R.name === 'gestao' && volta !== cid) { /* mantém assinatura */ } }
}
ACT.pausarG = async (el) => {
  const h = contagem(el.dataset.cid); if (!h) return;
  try {
    await comSync(() => S.db.doc(`contagens/${h.id}`).update({ status: 'pausada', atualizadoEm: Date.now(),
      eventos: [...(h.eventos || []), { t: Date.now(), p: S.me || '', d: 'Contagem pausada' }].slice(-50) }));
    toast('Contagem pausada — lançamentos guardados');
  } catch (e) { toast(errMsg(e), 4000); }
};
ACT.incluirAmbG = (el) => comContagem(el.dataset.cid, () => ACT.incluirAmb(el));
ACT.tirarAmbMenu = async (el) => {
  const h = contagem(el.dataset.cid); if (!h) return;
  const amb = await modal({ title: 'Tirar qual ambiente?', body: '<p class="small muted">Os lançamentos desse ambiente nesta contagem são apagados.</p>',
    actions: [...(h.ambientes || []).map((a) => ({ label: ambNome(a, h), value: a })), { label: 'Cancelar', value: null, cls: 'ghost' }] });
  if (!amb) return;
  ensureContagem(h.id);
  await ACT.tirarAmb({ dataset: { amb } });
};

function cardContagem(c) {
  const fech = fechadaStatus(c.status);
  const pill = c.status === 'finalizada' ? 'p-ok' : c.status === 'parcial' ? 'p-warn' : c.status === 'pausada' ? 'p-pend' : 'p-acc';
  const to = fech ? `#/c/${c.id}/resumo` : `#/c/${c.id}`;
  return `<button class="card tap lift" data-act="go" data-to="${to}">
    <div class="row between"><h3>${rotuloC(c)}</h3><span class="pill ${pill}">${STATUS_LABEL[c.status] || c.status}</span></div>
    <div class="muted small">Responsável: ${esc(c.responsavel)} · ${(c.ambientes || []).map((a) => esc(ambNome(a, c))).join(', ')}</div>
  </button>`;
}

/* ---- Nova contagem ---- */
const contagemAberta = () => S.contagens.filter((c) => !fechadaStatus(c.status)).sort((a, b) => (b.criadoEm || 0) - (a.criadoEm || 0))[0] || null;
VIEWS.nova = () => {
  const ab = contagemAberta();
  if (ab) return `${topbar({ back: '#/', eyebrow: 'Nova contagem', title: 'Já existe uma contagem aberta' })}
  <div class="stack" style="margin-top:16px">
    <div class="note n-info">A contagem de <b>${dataBR(ab.data)}</b> (responsável ${esc(ab.responsavel)}) está ${ab.status === 'pausada' ? 'pausada' : 'em andamento'}. Só é possível ter uma contagem aberta por vez.</div>
    <button class="btn big pri full" data-act="go" data-to="#/c/${ab.id}">Continuar essa contagem</button>
    <p class="small muted" style="margin:0">Para começar outra, finalize (em Revisar contagem) ou exclua a atual.</p>
  </div>`;
  const n = S.ui.novo || (S.ui.novo = { data: hojeISO(), resp: S.me || (S.user && S.user.nome) || '', ambs: ambAtivos().map((a) => a.id), tipo: tipoPorData(hojeISO()), par: '' });
  const cands = n.tipo === 'segunda' ? domingosCandidatos(n.data) : [];
  if (n.tipo === 'segunda' && !cands.some((c) => c.id === n.par)) n.par = cands[0] ? cands[0].id : '';
  const parH = n.par && contagem(n.par);
  return `${topbar({ back: '#/', eyebrow: 'Nova contagem', title: 'Iniciar contagem' })}
  <div class="stack" style="margin-top:16px">
    <label class="f">Data da contagem<input type="date" id="n-data" data-ch="novoData" value="${esc(n.data)}"></label>
    <div class="eyebrow">Tipo de contagem</div>
    <div class="seg">${[['domingo', 'Domingo'], ['segunda', 'Segunda (conferência)'], ['extra', 'Extra']].map(([k, l]) =>
      `<button data-act="novoTipo" data-k="${k}" class="${n.tipo === k ? 'on' : ''}">${l}</button>`).join('')}</div>
    ${n.tipo === 'segunda' ? (cands.length ? `<label class="f">Confere a contagem de<select id="n-par" data-ch="novoPar">${cands.map((c) =>
      `<option value="${c.id}" ${n.par === c.id ? 'selected' : ''}>${esc(rotuloC(c))} · ${esc(c.responsavel)}</option>`).join('')}</select></label>
      <p class="small muted" style="margin:0">Durante a contagem, o número do domingo aparece ao lado de cada item só depois que você lançar. No fim, abre a comparação Domingo × Segunda.</p>
      ${parH && segundaDe(parH.id) ? `<div class="note n-warn small">Esse domingo já tem uma conferência (${esc(rotuloC(segundaDe(parH.id)))}). Uma nova substitui a comparação.</div>` : ''}`
      : '<div class="note n-warn small">Não há contagem de domingo finalizada para conferir. Finalize a de domingo antes, ou faça esta como “Extra”.</div>') : ''}
    <label class="f">Responsável<input id="n-resp" data-in="novo" data-k="resp" value="${esc(n.resp)}" placeholder="Nome de quem responde pela contagem" autocomplete="name"></label>
    <div class="eyebrow">Ambientes desta contagem</div>
    <div class="list">${ambAtivos().map((a) => {
      const np = [...S.produtos.values()].filter((p) => p.ativo !== false && p.amb && p.amb[a.id]).length;
      return `<label class="li check"><input type="checkbox" id="n-a-${a.id}" data-ch="novoAmb" value="${a.id}" ${n.ambs.includes(a.id) ? 'checked' : ''}>
        <span class="grow"><span class="nm">${esc(a.nome)}</span><br><span class="meta">${np} produtos</span></span></label>`;
    }).join('')}</div>
    <p class="muted small" style="margin:0">A contagem começa vazia: nenhum valor de contagens anteriores é copiado.</p>
    <button class="btn big pri full" data-act="criarContagem" id="btn-criar">Começar</button>
  </div>`;
};
INP.novo = (el) => { S.ui.novo[el.dataset.k] = el.value; };
INP.novoData = (el) => { S.ui.novo.data = el.value; if (el.value) S.ui.novo.tipo = tipoPorData(el.value); S.ui.novo.par = ''; render(); };
ACT.novoTipo = (el) => { S.ui.novo.tipo = el.dataset.k; S.ui.novo.par = ''; render(); };
INP.novoPar = (el) => { S.ui.novo.par = el.value; render(); };
INP.novoAmb = (el) => {
  const s = new Set(S.ui.novo.ambs);
  el.checked ? s.add(el.value) : s.delete(el.value);
  S.ui.novo.ambs = [...s];
};
ACT.criarContagem = async (btn) => {
  const n = S.ui.novo;
  if (!n.data) return toast('Informe a data da contagem');
  if (!n.resp.trim()) { toast('Informe o responsável'); $('#n-resp').focus(); return; }
  if (!n.ambs.length) return toast('Marque pelo menos um ambiente para contar');
  if (n.tipo === 'segunda' && !n.par) return toast('Escolha a contagem de domingo que esta segunda confere, ou marque Extra', 4000);
  if (S.saving) return;
  const ab = contagemAberta();
  if (ab) { toast('Já existe uma contagem aberta — continuando nela'); go(`#/c/${ab.id}`); return; }
  S.saving = true; btn.disabled = true; btn.textContent = 'Criando…';
  const id = 'c' + n.data.replace(/-/g, '') + '-' + Math.random().toString(36).slice(2, 7);
  const ambs = ambAtivos().filter((a) => n.ambs.includes(a.id));
  try {
    await comSync(async () => {
      for (const a of ambs) await S.db.doc(`contagens/${id}/lanc/${a.id}`).set({ i: {} });
      await S.db.doc(`contagens/${id}`).set({
        data: n.data, tipo: n.tipo || 'extra', par: n.tipo === 'segunda' ? n.par : null, responsavel: n.resp.trim(), ambientes: ambs.map((a) => a.id),
        ambNomes: Object.fromEntries(ambs.map((a) => [a.id, a.nome])), status: 'andamento',
        criadoEm: Date.now(), atualizadoEm: Date.now(), eventos: [{ t: Date.now(), p: n.resp.trim(), d: 'Contagem iniciada' }],
      });
    });
    if (!S.me) { S.me = n.resp.trim(); LS.set('contador', S.me); }
    S.ui.novo = null;
    toast('Contagem criada');
    go(`#/c/${id}`);
  } catch (e) { toast(errMsg(e), 4000); }
  finally { S.saving = false; }
};

/* ---- Hub da contagem: escolher ambiente ---- */
VIEWS.hub = (r) => {
  const h = contagem(r.cid);
  if (!h) return `${topbar({ back: '#/', title: 'Contagem não encontrada' })}<p class="muted">Ela pode ter sido excluída.</p>`;
  if (S.ui.finalizando) return `<div class="loading"><div><img src="${LOGO}" alt=""><p class="eyebrow" style="margin-top:14px">Finalizando e gerando a planilha…</p></div></div>`;
  if (fechadaStatus(h.status)) { AFTER.push(() => route().name === R.name && go(ehGestor() ? `#/c/${h.id}/resumo` : '#/')); return ''; }
  if (h.status === 'pausada') { AFTER.push(() => route().name === R.name && go('#/')); return ''; }
  const ctx = ctxAtual();
  let totC = 0, totT = 0, carregado = true;
  const cards = ctx.ambs.map((a) => {
    const pr = progresso(ctx.itens(a.id));
    const ok = S.cur.lancOk[a.id];
    if (!ok) carregado = false;
    totC += pr.contados; totT += pr.total;
    const fim = ok && pr.total && pr.contados === pr.total;
    return `<div class="card lift amb-card ambwrap">
      <button class="ambgo" data-act="entrarAmb" data-amb="${a.id}">
        <div class="row between"><h2>${esc(a.nome)}</h2>${fim ? '<span class="feito-ok">✓ concluído</span>' : `<span class="num small muted">${ok ? `${pr.contados}/${pr.total}` : '…'}</span>`}</div>
        <div class="prog" style="margin-top:10px"><i style="width:${pr.pct}%"></i></div>
      </button></div>`;
  }).join('');
  const hp = h.par ? contagem(h.par) : null;
  if (hp) ctxParDe(h);
  return `${topbar({ back: '#/', eyebrow: `Contagem de ${rotuloC(h)}`, title: 'Escolha o ambiente' })}
  <div class="stack" style="margin-top:14px">
    ${hp ? `<div class="note n-info small"><b>Conferência de ${esc(rotuloC(hp))}</b> (responsável ${esc(hp.responsavel)}). Depois de lançar cada item, aparece o número do domingo e a diferença; ⚠ marca o que passou do limite.</div>` : ''}
    ${cards}
    ${ehGestor() && ambAtivos().some((a) => !(h.ambientes || []).includes(a.id)) ? `<button class="btn full" data-act="incluirAmb" style="border-style:dashed">+ Incluir outro ambiente</button>` : ''}
  </div>
  ${ehGestor() ? `<div class="bar"><div class="in"><button class="btn big pri" data-act="finalizarRapido" ${carregado ? '' : 'disabled'}>Finalizar contagem${totT ? ` · ${totC}/${totT}` : ''}</button></div></div>` : ''}`;
};
ACT.trocarNome = async () => {
  const v = await pedirNome(true);
  if (v) render();
};
async function pedirNome(force) {
  if (S.user && !force) { S.me = S.user.nome; return S.me; }
  if (S.me && !force) return S.me;
  const h = contagem(S.cur.cid);
  const v = await modal({ title: 'Quem está contando?', body: `<p class="muted small">Seu nome fica registrado em cada lançamento feito neste aparelho.</p>
    <label class="f">Nome<input id="m-nome" value="${esc(S.me || (h && h.responsavel) || '')}" autocomplete="name"></label>`,
    actions: [{ label: 'Confirmar', cls: 'pri', check: (m) => !!m.querySelector('#m-nome').value.trim(), collect: (m) => m.querySelector('#m-nome').value.trim() }, { label: 'Cancelar', value: null, cls: 'ghost' }] });
  if (v) { S.me = v; LS.set('contador', v); }
  return v;
}
ACT.entrarAmb = async (el) => {
  if (!(await pedirNome(false))) return;
  S.ui.busca = ''; S.ui.cat = ''; S.ui.filtro = 'pend'; S.ui.recem = new Set(); S.ui.abKey = null; S.ui.abD = null;
  go(`#/c/${S.cur.cid}/a/${el.dataset.amb}`);
};
ACT.setStatus = async (el) => {
  const h = contagem(S.cur.cid); const st = el.dataset.st;
  try {
    await comSync(() => S.db.doc(`contagens/${h.id}`).update({ status: st, atualizadoEm: Date.now(),
      eventos: [...(h.eventos || []), { t: Date.now(), p: S.me || '', d: st === 'pausada' ? 'Contagem pausada' : 'Contagem retomada' }].slice(-50) }));
    toast(st === 'pausada' ? 'Contagem pausada — lançamentos guardados' : 'Contagem retomada');
    if (st === 'pausada') go('#/');
  } catch (e) { toast(errMsg(e), 4000); }
};
ACT.excluirContagem = async (el) => {
  if (!ehGestor()) return toast('Só o gestor exclui uma contagem');
  const h = contagem(el.dataset.cid);
  if (!h) return;
  const ok = await modal({ title: 'Excluir contagem?', body: `<p>A contagem de <b>${dataBR(h.data)}</b> (responsável ${esc(h.responsavel)}) e todos os seus lançamentos serão apagados para todos. Não dá para desfazer.</p>
    <label class="check"><input type="checkbox" id="m-ok"> Entendo que isso apaga a contagem definitivamente</label>`,
    actions: [{ label: 'Excluir definitivamente', cls: 'dan', check: (m) => m.querySelector('#m-ok').checked || (toast('Marque a confirmação'), false), value: true }, { label: 'Cancelar', value: false, cls: 'ghost' }] });
  if (!ok) return;
  try {
    await comSync(async () => {
      for (const a of h.ambientes || []) await S.db.doc(`contagens/${h.id}/lanc/${a}`).delete();
      await S.db.doc(`contagens/${h.id}/snap/params`).delete();
      await S.db.doc(`contagens/${h.id}`).delete();
    });
    toast('Contagem excluída');
    go('#/');
  } catch (e) { toast(errMsg(e), 4000); }
};

/* ---- Lista de produtos do ambiente ---- */
function filtrarItens(lista) {
  const termos = normBusca(S.ui.busca.trim()).split(/\s+/).filter(Boolean);
  const buscando = termos.length > 0;
  const com = lista.filter((it) => {
    if (S.ui.cat && it.categoria !== S.ui.cat) return false;
    // durante a busca, mostra qualquer item (contado ou não) para achar pelo nome
    if (!buscando) {
      if (S.ui.filtro === 'pend' && it.r.status !== 'nao_contado' && !(S.ui.recem && S.ui.recem.has(it.key))) return false;
      if (S.ui.filtro === 'cont' && it.r.status === 'nao_contado') return false;
    }
    if (!buscando) return true;
    it._score = pontuaBusca(it, termos);
    return it._score > 0;
  });
  if (buscando) com.sort((a, b) => b._score - a._score || collator.compare(a.descricao, b.descricao));
  return com;
}
/* pontua: começa com o que foi digitado > começo de palavra > contém */
function pontuaBusca(it, termos) {
  const desc = normBusca(`${it.descricao} ${it.varNome || ''}`);
  const cod = normBusca(it.codigo);
  const palavras = desc.split(/[^a-z0-9]+/).filter(Boolean);
  let total = 0;
  for (const t of termos) {
    let p = 0;
    if (desc.startsWith(t)) p = 100;
    else if (palavras.some((w) => w.startsWith(t))) p = 60;
    else if (cod.startsWith(t)) p = 50;
    else if (desc.includes(t)) p = 20;
    else if (cod.includes(t)) p = 10;
    if (!p) return 0;
    total += p;
  }
  return total;
}
function resumoCurto(it) {
  const r = it.r;
  if (r.status === 'nao_contado') return '<span class="muted">—</span>';
  if (r.status === 'sem_estoque') return '<span class="pill p-zero">Sem estoque</span>';
  const parts = [];
  if (it.forma !== 'peso') parts.push(`${r.fechadas} fech.`);
  if (r.nAbertas) parts.push(`${r.nAbertas} ab.`);
  let tot = '';
  if (r.totalOk && it.unidade !== 'UNID') tot = `<br><b>${FA(r.total, 3)} ${r.totalRotulo}</b>`;
  return `${parts.join(' + ')}${tot}`;
}
function statusDot(it) {
  const r = it.r;
  if (r.status === 'nao_contado') return '<span class="dot d-pend" title="Não contado"></span>';
  if (r.erros.length || r.pendencias.length || r.alertas.length) return '<span class="dot d-warn" title="Contado com pendência"></span>';
  if (r.status === 'sem_estoque') return '<span class="dot d-zero" title="Sem estoque"></span>';
  return '<span class="dot d-ok" title="Contado"></span>';
}
VIEWS.amb = (r) => {
  const h = contagem(r.cid);
  if (!h) return `${topbar({ back: '#/', title: 'Contagem não encontrada' })}`;
  if (fechadaStatus(h.status)) { AFTER.push(() => route().name === R.name && go(ehGestor() ? `#/c/${h.id}/resumo` : '#/')); return ''; }
  const ctx = ctxAtual();
  const nome = ambNome(r.amb, h);
  const lista = ctx.itens(r.amb);
  const pr = progresso(lista);
  S.ui.filtro = 'todos';
  const cats = [...new Set(lista.map((x) => x.categoria))];
  if (S.ui.cat && !cats.includes(S.ui.cat)) S.ui.cat = '';
  return `${topbar({ back: `#/c/${h.id}`, eyebrow: `${pr.contados} de ${pr.total} contados`, title: esc(nome),
    extra: `<div class="prog" style="margin-top:10px"><i style="width:${pr.pct}%"></i></div>
      <div class="srch">${ICON.search}<input id="busca" type="search" data-in="busca" placeholder="Buscar produto ou código" value="${esc(S.ui.busca)}" autocomplete="off"></div>
      <div class="chips cats" style="margin-top:8px"><button class="chip ${!S.ui.cat ? 'on' : ''}" data-act="catChip" data-k="">Todas</button>${cats.map((c) => {
        const n = lista.filter((x) => x.categoria === c && x.r.status === 'nao_contado').length;
        return `<button class="chip ${S.ui.cat === c ? 'on' : ''}" data-act="catChip" data-k="${esc(c)}">${esc(cap(c.toLowerCase()))}${n ? ` · ${n}` : ' ✓'}</button>`; }).join('')}</div>` })}
  <div id="lst">${listaHTML(lista, r)}</div>
  <div class="bar"><div class="in"><button class="btn big pri" data-act="concluirAmb">Concluir ${esc(nome)}</button></div></div>`;
};
ACT.concluirAmb = () => {
  const ctx = ctxAtual(); if (!ctx) return;
  const pr = progresso(ctx.itens(R.amb));
  const falta = pr.total - pr.contados;
  toast(falta ? `${ambNome(R.amb, ctx.h)} salvo · ${falta} ${falta === 1 ? 'item ficou' : 'itens ficaram'} sem contar` : `${ambNome(R.amb, ctx.h)} concluído ✓`, 3000);
  S.ui.busca = '';
  go(`#/c/${R.cid}`);
};
ACT.catChip = (el) => { S.ui.cat = el.dataset.k; render(); };
function listaHTML(lista, r) {
  const f = filtrarItens(lista);
  if (!f.length) return `<div class="card muted" style="text-align:center;margin-top:14px">Nenhum produto encontrado.</div>`;
  const ctxP = ctxParDe(contagem(R.cid));
  let out = '', cat = null, open = false;
  const buscando = !!S.ui.busca.trim();
  if (buscando) {
    out += `<div class="list" style="margin-top:14px">`;
    open = true; cat = '\u0000';
  }
  for (const it of f) {
    if (!buscando && it.categoria !== cat) {
      if (open) out += '</div>';
      cat = it.categoria; out += `<div class="eyebrow" style="margin:14px 0 6px">${esc(cat)}</div><div class="list">`; open = true;
    }
    const rr = it.r;
    const E = embL(it.embalagem);
    const val = rr.status === 'nao_contado' ? '' : (isNum(rr.fechadas) ? String(rr.fechadas) : '');
    const aberto = S.ui.abKey === it.key;
    const soPeso = it.forma === 'peso';
    const pesoTxt = rr.status === 'sem_estoque' ? '0 kg' : rr.status !== 'nao_contado' && rr.nAbertas && isNum(rr.kgAbertas) ? `${FA(rr.kgAbertas, 3)} kg` : '';
    const ab = it.forma === 'fechadas' ? '' :
      `<button class="btn qab ${soPeso ? 'wide' : ''} ${rr.nAbertas ? 'pri' : ''} ${aberto ? 'on' : ''}" data-act="togAb" data-key="${esc(it.key)}" aria-expanded="${aberto}" aria-label="Pesagens de ${esc(it.descricao)}">${ICON.garrafa}${soPeso ? `<b>${pesoTxt || 'Pesar'}</b>` : rr.nAbertas ? `<b>${rr.nAbertas}</b>` : ''}</button>`;
    const nomeAct = ehGestor() ? 'abrirItem' : it.forma === 'fechadas' ? 'focoQtd' : 'togAb';
    const qtd = soPeso ? '' : `<input class="qin" id="q-${esc(it.key)}" inputmode="numeric" pattern="[0-9]*" data-ch="qf" data-key="${esc(it.key)}"
        value="${esc(val)}" placeholder="—" aria-label="${esc(E.f)} de ${esc(it.descricao)}" autocomplete="off">
      <span class="qstep">
        <button data-act="qstep" data-key="${esc(it.key)}" data-d="1" aria-label="Somar 1 em ${esc(it.descricao)}">▲</button>
        <button data-act="qstep" data-key="${esc(it.key)}" data-d="-1" aria-label="Tirar 1 de ${esc(it.descricao)}">▼</button>
      </span>`;
    const dica = soPeso ? '' : it.unidade === 'UNID' ? 'unidades' : `${E.pl}${isNum(it.p.pesoLiqG) ? ` de ${F(it.p.pesoLiqG)} g` : ''}${rr.nAbertas && isNum(rr.kgAbertas) ? ` + ${FA(rr.kgAbertas, 3)} kg pesados` : ''}`;
    out += `<div class="li qrow ${rr.status !== 'nao_contado' ? 'feito' : ''}" data-key="${esc(it.key)}">
      <button class="qthumb" data-act="fotoItem" data-key="${esc(it.key)}" aria-label="Foto de ${esc(it.descricao)}">${miniHTML(it)}</button>
      <button class="qname" data-act="${nomeAct}" data-key="${esc(it.key)}">
        <span class="nm">${esc(it.descricao)}${it.varNome ? ` · <span class="muted">${esc(it.varNome)}</span>` : ''}</span>
        ${rr.status === 'sem_estoque' ? '<br><span class="meta">sem estoque</span>' : dica ? `<br><span class="meta">${esc(dica)}</span>` : ''}
        ${linhaPar(it, ctxP, R.amb)}
      </button>
      ${qtd}
      ${ab}</div>${aberto ? painelAbertas(it) : ''}`;
  }
  return out + (open ? '</div>' : '');
}
/* lançamento rápido pela lista (fechadas / unidades) */
const salvando = new Set();
const debounceQ = {};
async function salvarQtd(key, valor, el) {
  const ctx = ctxAtual(); if (!ctx) return;
  const it = ctx.itens(R.amb).find((x) => x.key === key); if (!it) return;
  if (salvando.has(key)) return;
  const atual = it.r.status === 'nao_contado' ? null : it.r.fechadas;
  if (atual === valor && it.r.status !== 'nao_contado') return;
  if (!(await pedirNome(false))) return;
  salvando.add(key);
  const e = it.e || {};
  const semEstoque = valor === 0 && (it.forma === 'fechadas' || !(e.a || []).length);
  const now = Date.now();
  const E = embL(it.embalagem);
  const entry = { c: true, s: semEstoque, f: valor, a: semEstoque ? [] : (e.a || []), o: e.o || '', u: now, p: S.me || '',
    h: [...(e.h || []), { t: now, p: S.me || '', d: `${e && e.c ? 'Alterado' : 'Lançado'} na lista: ${semEstoque ? 'sem estoque' : valor + ' ' + E.pl}` }].slice(-4) };
  try {
    await comSync(async () => {
      const ref = S.db.doc(`contagens/${R.cid}/lanc/${R.amb}`);
      try { await ref.update({ i: { [key]: entry } }); }
      catch (err) {
        if (err && err.code === 'invalid_argument') { const sn = await ref.get(); if (!sn.exists) { await ref.set({ i: { [key]: entry } }); return; } }
        throw err;
      }
    });
    S.ui.recem = S.ui.recem || new Set();
    S.ui.recem.add(key);
    const inp = el && el.isConnected ? el : document.getElementById('q-' + key);
    if (inp) { inp.classList.remove('bad'); inp.classList.add('ok'); }
  } catch (err) { toast(errMsg(err), 4000); const inp = document.getElementById('q-' + key); if (inp) inp.classList.add('bad'); }
  finally { salvando.delete(key); }
}
INP.qf = (el) => {
  const key = el.dataset.key;
  const txt = el.value.trim();
  if (txt === '') { el.classList.remove('bad', 'ok'); return; }
  const pf = Calc.parseNum(txt, { integer: true });
  if (!pf.ok) { el.classList.add('bad'); toast(pf.error, 3000); return; }
  el.classList.remove('bad');
  clearTimeout(debounceQ[key]);
  salvarQtd(key, pf.value, el);
};
ACT.focoQtd = (el) => { const i = document.getElementById('q-' + el.dataset.key); if (i) { i.focus(); i.select && i.select(); } };
ACT.qstep = (el) => {
  const key = el.dataset.key, d = Number(el.dataset.d);
  const inp = document.getElementById('q-' + key); if (!inp) return;
  const pf = Calc.parseNum(inp.value.trim(), { integer: true });
  const base = pf.ok && isNum(pf.value) ? pf.value : 0;
  const novo = Math.max(0, base + d);
  inp.value = String(novo);
  inp.classList.remove('bad'); inp.classList.add('ok');
  clearTimeout(debounceQ[key]);
  debounceQ[key] = setTimeout(() => salvarQtd(key, novo, inp), 700);
};

/* ---- Pesagens direto na lista (em kg, com o recipiente de cada uma) ---- */
function painelAbertas(it) {
  const E = embL(it.embalagem);
  const d = S.ui.abD || [];
  const linhas = d.map((g, ix) => {
    const nome = `Pesagem ${ix + 1}`;
    return `<div class="abrow">
      <span class="abn">${nome}</span>
      <div class="unitwrap"><input id="gq-${esc(g.i)}" inputmode="decimal" data-in="gqb" data-i="${esc(g.i)}" data-ch="gqsave"
        value="${esc(g.bText)}" placeholder="peso da balança" autocomplete="off"><span class="u">kg</span></div>
      <button class="btn ghost abx" data-act="gqdel" data-i="${esc(g.i)}" aria-label="Excluir ${nome}">✕</button>
      ${recipSelect(g, it, `data-ch="gqt" data-i="${esc(g.i)}" id="gt-${esc(g.i)}"`)}
      <div class="abres" id="gqr-${esc(g.i)}">${abResumo(g, it)}</div>
    </div>`;
  }).join('');
  return `<div class="abpanel">
    <div class="small muted">${it.forma === 'peso' ? 'Pese tudo o que tem deste item aqui. Uma pesagem por pote/saco, ou tudo junto.' : `Pese as ${E.pl} abertas ou sobras. As fechadas você conta no número ao lado.`} Escolha o recipiente para descontar a tara.</div>
    ${linhas || '<div class="small muted">Nenhuma pesagem lançada.</div>'}
    <div class="row">
      <button class="btn grow" data-act="gqadd" data-key="${esc(it.key)}">+ Pesagem</button>
      <button class="btn ghost" data-act="togAb" data-key="${esc(it.key)}">Fechar</button>
    </div>
  </div>`;
}
function abResumo(g, it) {
  const pb = parsePeso(g.bText);
  if (!pb.ok) return `<span style="color:var(--danger)">${esc(pb.error)}</span>`;
  if (pb.empty) return '<span class="muted">digite o peso da balança (kg)</span>';
  const a = Calc.aberta({ b: pb.value, t: g.t }, it.p, 'KG');
  if (a.erro) return `<span style="color:var(--danger)">${esc(a.erro)}</span>`;
  if (!isNum(a.kg)) return '<span style="color:var(--amber)">escolha o recipiente para descontar a tara</span>';
  return `<span class="muted">líquido <b>${FA(a.kg, 3)} kg</b>${isNum(a.taraG) && a.taraG ? ` (− ${F(a.taraG)} g de tara)` : ''}</span>`;
}
const draftDeLanc = (e) => ((!e.s && e.a) || []).map((g) => ({ i: g.i || uid('g'), bText: kgTxt(g.b), t: isNum(g.t) ? g.t : null, reg: g.r || null }));
const taraInicial = (it) => (isNum(it.p.taraG) ? it.p.taraG : null);
ACT.gqaddFast = async (el) => {
  const key = el.dataset.key;
  if (S.ui.abKey !== key) {
    if (!(await pedirNome(false))) return;
    const it0 = ctxAtual().itens(R.amb).find((x) => x.key === key); if (!it0) return;
    S.ui.abKey = key;
    S.ui.abD = draftDeLanc(it0.e || {});
    S.ui.recem = S.ui.recem || new Set(); S.ui.recem.add(key);
  }
  const it = ctxAtual().itens(R.amb).find((x) => x.key === key); if (!it) return;
  const ult = (S.ui.abD || [])[S.ui.abD.length - 1];
  const g = { i: uid('g'), bText: '', t: ult && isNum(ult.t) ? ult.t : taraInicial(it), reg: null };
  S.ui.abD = [...(S.ui.abD || []), g];
  render();
  const inp = document.getElementById('gq-' + g.i);
  if (inp) { inp.focus(); inp.scrollIntoView({ block: 'center' }); }
};
ACT.togAb = async (el) => {
  const key = el.dataset.key;
  if (S.ui.abKey === key) { await salvarAbertas(); S.ui.abKey = null; S.ui.abD = null; render(); return; }
  if (S.ui.abKey) await salvarAbertas();
  if (!(await pedirNome(false))) return;
  const it = ctxAtual().itens(R.amb).find((x) => x.key === key); if (!it) return;
  S.ui.abKey = key;
  S.ui.abD = draftDeLanc(it.e || {});
  if (!S.ui.abD.length) S.ui.abD.push({ i: uid('g'), bText: '', t: taraInicial(it), reg: null });
  S.ui.recem = S.ui.recem || new Set(); S.ui.recem.add(key);
  render();
  const primeiro = S.ui.abD[S.ui.abD.length - 1];
  const inp = document.getElementById('gq-' + primeiro.i);
  if (inp) { inp.focus(); inp.scrollIntoView({ block: 'center' }); }
};
INP.gqb = (el) => {
  const g = (S.ui.abD || []).find((x) => x.i === el.dataset.i); if (!g) return;
  g.bText = el.value.trim();
  const it = ctxAtual().itens(R.amb).find((x) => x.key === S.ui.abKey);
  const box = document.getElementById('gqr-' + g.i);
  if (box && it) box.innerHTML = abResumo(g, it);
};
INP.gqt = (el) => {
  const g = (S.ui.abD || []).find((x) => x.i === el.dataset.i); if (!g) return;
  g.t = el.value === '' ? null : Number(el.value);
  const it = ctxAtual().itens(R.amb).find((x) => x.key === S.ui.abKey);
  const box = document.getElementById('gqr-' + g.i);
  if (box && it) box.innerHTML = abResumo(g, it);
  salvarAbertas();
};
INP.gqsave = () => salvarAbertas();
ACT.gqadd = (el) => {
  const it = ctxAtual().itens(R.amb).find((x) => x.key === el.dataset.key); if (!it) return;
  const ult = (S.ui.abD || [])[S.ui.abD.length - 1];
  const g = { i: uid('g'), bText: '', t: ult && isNum(ult.t) ? ult.t : taraInicial(it), reg: null };
  S.ui.abD = [...(S.ui.abD || []), g];
  render();
  const inp = document.getElementById('gq-' + g.i);
  if (inp) { inp.focus(); inp.scrollIntoView({ block: 'center' }); }
};
ACT.gqdel = async (el) => {
  const ix = (S.ui.abD || []).findIndex((x) => x.i === el.dataset.i); if (ix < 0) return;
  const g = S.ui.abD[ix];
  if (g.bText && !(await confirmar('Excluir pesagem?', `Remover a pesagem de ${esc(g.bText)} kg?`, 'Excluir', 'dan'))) return;
  S.ui.abD.splice(ix, 1);
  render();
  salvarAbertas(true);
};
async function salvarAbertas(permitirVazio) {
  const key = S.ui.abKey; if (!key) return;
  const ctx = ctxAtual(); if (!ctx) return;
  const it = ctx.itens(R.amb).find((x) => x.key === key); if (!it) return;
  if (salvando.has(key)) return;
  const a = []; const erros = [];
  (S.ui.abD || []).forEach((g, ix) => {
    const pb = parsePeso(g.bText);
    if (pb.empty) return;
    if (!pb.ok) { erros.push(`Pesagem ${ix + 1}: ${pb.error}`); return; }
    const t = isNum(g.t) ? g.t : null;
    if (isNum(t) && pb.value < t) { erros.push(`Pesagem ${ix + 1}: o peso (${FA(pb.value / 1000, 3)} kg) é menor que a tara do recipiente (${F(t)} g).`); return; }
    a.push({ i: g.i, b: pb.value, t, r: g.reg || Date.now() });
  });
  if (erros.length) { toast(erros[0], 4000); return; }
  const e = it.e || {};
  if (!a.length && !e.c && !permitirVazio) return;
  const igual = JSON.stringify((e.a || []).map((g) => [g.b, g.t])) === JSON.stringify(a.map((g) => [g.b, g.t]));
  if (igual && e.c) return;
  const inp = document.getElementById('q-' + key);
  const pf = inp ? Calc.parseNum(inp.value.trim(), { integer: true }) : { empty: true };
  const f = it.forma === 'peso' ? null : pf.ok && !pf.empty ? pf.value : (e.c && !e.s && isNum(e.f) ? e.f : (a.length ? 0 : null));
  if (!a.length && it.forma === 'peso') {
    // apagou todas as pesagens: volta para não contado
    try { await comSync(() => S.db.doc(`contagens/${R.cid}/lanc/${R.amb}`).update({ i: { [key]: null } })); } catch (err) { toast(errMsg(err), 4000); }
    return;
  }
  const now = Date.now();
  salvando.add(key);
  const entry = { c: true, s: false, f, a, o: e.o || '', u: now, p: S.me || '',
    h: [...(e.h || []), { t: now, p: S.me || '', d: `${e && e.c ? 'Alterado' : 'Lançado'} na lista: ${a.length} ${a.length === 1 ? 'pesagem' : 'pesagens'} (${a.map((g) => FA(g.b / 1000, 3) + ' kg').join(', ')})` }].slice(-4) };
  try {
    await comSync(async () => {
      const ref = S.db.doc(`contagens/${R.cid}/lanc/${R.amb}`);
      try { await ref.update({ i: { [key]: entry } }); }
      catch (err) {
        if (err && err.code === 'invalid_argument') { const sn = await ref.get(); if (!sn.exists) { await ref.set({ i: { [key]: entry } }); return; } }
        throw err;
      }
    });
  } catch (err) { toast(errMsg(err), 4000); }
  finally { salvando.delete(key); }
}

document.addEventListener('keydown', (ev) => {
  const el = ev.target.closest && ev.target.closest('input.qin');
  if (!el || ev.key !== 'Enter') return;
  ev.preventDefault();
  const inputs = $$('input.qin');
  const prox = inputs[inputs.indexOf(el) + 1];
  S.ui.focarProx = prox ? prox.id : null;
  el.blur();
  setTimeout(focarProximo, 120);
});
function focarProximo() {
  const id = S.ui.focarProx; if (!id) return;
  const el = document.getElementById(id);
  if (el) { S.ui.focarProx = null; el.focus(); el.select && el.select(); el.scrollIntoView({ block: 'center' }); }
}
INP.busca = (el) => {
  S.ui.busca = el.value;
  const ctx = ctxAtual(); if (!ctx) return;
  const lst = $('#lst'); if (lst) lst.innerHTML = listaHTML(ctx.itens(R.amb), R);
};
INP.cat = (el) => { S.ui.cat = el.value; render(); };
ACT.filtro = (el) => { S.ui.filtro = el.dataset.k; S.ui.recem = new Set(); render(); };
ACT.abrirItem = (el) => {
  S.ui.listaOrdem = filtrarItens(ctxAtual().itens(R.amb)).map((x) => x.key);
  go(`#/c/${R.cid}/a/${R.amb}/i/${encodeURIComponent(el.dataset.key)}`);
};

/* ===== Foto do produto (miniatura na lista) ===== */
function fotoSrc(o) { return (o && (o.fotoMini || o.fotoUrl)) || ''; }
function miniHTML(o) {
  const src = fotoSrc(o);
  if (src) return `<img src="${esc(src)}" alt="" loading="lazy">`;
  const letra = (o.descricao || '?').trim().charAt(0).toUpperCase();
  return `<span class="ini">${esc(letra)}</span>`;
}
/* reduz a imagem antes de guardar: miniatura leve, boa para reconhecer na prateleira */
async function miniatura(file, max = 200, q = 0.62) {
  const url = URL.createObjectURL(file);
  try {
    const img = await new Promise((ok, err) => { const i = new Image(); i.onload = () => ok(i); i.onerror = () => err(new Error('imagem inválida')); i.src = url; });
    const f = Math.min(1, max / Math.max(img.width, img.height));
    const w = Math.max(1, Math.round(img.width * f)), h = Math.max(1, Math.round(img.height * f));
    const c = document.createElement('canvas'); c.width = w; c.height = h;
    c.getContext('2d').drawImage(img, 0, 0, w, h);
    return c.toDataURL('image/jpeg', q);
  } finally { URL.revokeObjectURL(url); }
}
ACT.fotoItem = async (el) => {
  const codigo = (el.dataset.key || '').split('~')[0];
  const prod = S.produtos.get(codigo);
  if (!prod) return toast('Produto não encontrado no cadastro');
  let nova = null;
  const atual = fotoSrc(prod);
  const r = await modal({
    title: 'Foto do produto',
    body: `<p class="small muted">${esc(prod.descricao)}</p>
      <div id="fpv" class="stack" style="align-items:center">${atual ? `<img src="${esc(atual)}" alt="" style="max-width:150px;border-radius:12px">` : '<div class="small muted">Sem foto por enquanto.</div>'}</div>
      <label class="f">Tirar foto ou escolher do celular<input type="file" id="f-file" accept="image/*" capture="environment"></label>
      <p class="small muted">A foto entra reduzida e fica guardada no app, só para reconhecer o produto na prateleira.</p>`,
    onOpen: (m) => {
      const inp = m.querySelector('#f-file');
      if (!inp || inp.dataset.on) return;
      inp.dataset.on = '1';
      inp.addEventListener('change', async () => {
        const f = inp.files && inp.files[0];
        if (!f) return;
        try {
          nova = await miniatura(f);
          m.querySelector('#fpv').innerHTML = `<img src="${nova}" alt="" style="max-width:150px;border-radius:12px">`;
        } catch (e) { toast('Não consegui ler essa imagem'); }
      });
    },
    actions: [
      { label: 'Salvar foto', cls: 'pri', collect: (m) => ({ mini: nova }) },
      { label: 'Tirar a foto do produto', cls: 'ghost dan', value: { remover: true } },
      { label: 'Cancelar', value: null, cls: 'ghost' }],
  });
  if (!r) return;
  const patch = { atualizadoEm: Date.now(), atualizadoPor: S.me || '' };
  if (r.remover) { patch.fotoMini = null; patch.fotoUrl = ''; }
  else if (r.mini) patch.fotoMini = r.mini;
  else return;
  try {
    await comSync(() => S.db.doc(`produtos/${codigo}`).update(patch));
    if (S.ui.pdraft && S.ui.pdraft.codigo === codigo) {
      if ('fotoMini' in patch) S.ui.pdraft.fotoMini = patch.fotoMini;
      if ('fotoUrl' in patch) S.ui.pdraft.fotoUrl = patch.fotoUrl;
    }
    toast(r.remover ? 'Foto removida' : 'Foto salva ✓');
    render();
  } catch (e) { toast(errMsg(e), 4000); }
};


/* ---- Incluir um ambiente numa contagem já aberta ---- */
ACT.incluirAmb = async () => {
  if (!ehGestor()) return toast('Só o gestor muda os ambientes da contagem');
  const h = contagem(S.cur.cid);
  if (!h) return;
  if (fechadaStatus(h.status)) return toast('Esta contagem já foi encerrada');
  const faltam = ambAtivos().filter((a) => !(h.ambientes || []).includes(a.id));
  if (!faltam.length) return toast('Todos os ambientes já estão nesta contagem');
  const escolha = await modal({
    title: 'Incluir ambiente na contagem',
    body: `<div class="stack">${faltam.map((a) => `<label class="check"><input type="checkbox" class="m-amb" value="${esc(a.id)}"> ${esc(a.nome)}</label>`).join('')}</div>
      <p class="small muted">Entram nesta mesma contagem, cada um com a sua lista de produtos. O que já foi lançado não muda.</p>`,
    actions: [
      { label: 'Incluir', cls: 'pri',
        check: (m) => m.querySelectorAll('.m-amb:checked').length > 0 || (toast('Marque pelo menos um ambiente'), false),
        collect: (m) => [...m.querySelectorAll('.m-amb:checked')].map((x) => x.value) },
      { label: 'Cancelar', value: null, cls: 'ghost' }],
  });
  if (!escolha || !escolha.length) return;
  const novos = ambAtivos().filter((a) => escolha.includes(a.id));
  try {
    await comSync(async () => {
      for (const a of novos) await S.db.doc(`contagens/${h.id}/lanc/${a.id}`).set({ i: {} });
      await S.db.doc(`contagens/${h.id}`).update({
        ambientes: [...(h.ambientes || []), ...novos.map((a) => a.id)],
        ambNomes: { ...(h.ambNomes || {}), ...Object.fromEntries(novos.map((a) => [a.id, a.nome])) },
        eventos: [...(h.eventos || []), { t: Date.now(), p: S.me || '', d: `Ambiente incluído: ${novos.map((a) => a.nome).join(', ')}` }].slice(-50),
        atualizadoEm: Date.now(),
      });
    });
    const loc = S.contagens.find((c) => c.id === h.id);
    if (loc) {
      loc.ambientes = [...new Set([...(loc.ambientes || []), ...novos.map((a) => a.id)])];
      loc.ambNomes = { ...(loc.ambNomes || {}), ...Object.fromEntries(novos.map((a) => [a.id, a.nome])) };
    }
    novos.forEach((a) => assinarLanc(h.id, a.id));
    toast(novos.length === 1 ? `${novos[0].nome} entrou na contagem ✓` : `${novos.length} ambientes entraram na contagem ✓`, 3500);
    render();
  } catch (e) { toast(errMsg(e), 4000); }
};


/* ---- Tirar um ambiente da contagem, pelo ✕ do próprio card ---- */
ACT.tirarAmb = async (el) => {
  if (!ehGestor()) return toast('Só o gestor muda os ambientes da contagem');
  const h = contagem(S.cur.cid);
  if (!h) return;
  if (fechadaStatus(h.status)) return toast('Esta contagem já foi encerrada');
  const ambs = (h.ambientes || []);
  const alvo = el.dataset.amb;
  if (!alvo || !ambs.includes(alvo)) return;
  if (ambs.length <= 1) return toast('A contagem precisa de pelo menos um ambiente. Para apagar tudo, use “Excluir contagem”.', 5000);
  const nome = ambNome(alvo, h);
  const pr = S.cur.lancOk[alvo] ? progresso(ctxAtual().itens(alvo)) : null;
  const ok = await modal({
    title: `Tirar ${nome} da contagem?`,
    body: `<p>${pr && pr.contados ? `Este ambiente tem <b>${pr.contados} ${pr.contados === 1 ? 'item lançado' : 'itens lançados'}</b>, que serão apagados.` : 'Ainda não há nada lançado neste ambiente.'}</p>
      <p class="small muted">Os outros ambientes desta contagem não são afetados, e o cadastro dos produtos continua igual.</p>`,
    actions: [{ label: `Tirar ${nome}`, value: true, cls: 'dan' }, { label: 'Cancelar', value: false, cls: 'ghost' }],
  });
  if (!ok) return;
  try {
    await comSync(async () => {
      await S.db.doc(`contagens/${h.id}/lanc/${alvo}`).delete();
      await S.db.doc(`contagens/${h.id}`).update({
        ambientes: ambs.filter((x) => x !== alvo),
        eventos: [...(h.eventos || []), { t: Date.now(), p: S.me || '', d: `Ambiente retirado da contagem: ${nome}` }].slice(-50),
        atualizadoEm: Date.now(),
      });
    });
    const loc = S.contagens.find((c) => c.id === h.id);
    if (loc) loc.ambientes = ambs.filter((x) => x !== alvo);
    delete S.cur.lanc[alvo]; delete S.cur.lancOk[alvo];
    if (S.ui.resAmb === alvo) S.ui.resAmb = null;
    toast(`${nome} saiu desta contagem`, 3500);
    render();
  } catch (e) { toast(errMsg(e), 4000); }
};

/* ===== Lançamento de um item ===== */
const cap = (s) => s.charAt(0).toUpperCase() + s.slice(1);
const numIn = (n) => (isNum(n) ? String(n).replace('.', ',') : '');
const draftKey = (d) => `draft:${d.cid}:${d.amb}:${d.key}`;

function novoDraft(r, it) {
  const e = it.e;
  const d = {
    cid: r.cid, amb: r.amb, key: r.key, dirty: false, baseU: (e && e.u) || 0,
    s: !!(e && e.s), fText: e && !e.s && isNum(e.f) ? String(e.f) : '',
    a: ((e && !e.s && e.a) || []).map((g) => ({ i: g.i, bText: kgTxt(g.b), t: isNum(g.t) ? g.t : null, reg: g.r || null })),
    o: (e && e.o) || '', restored: false, remote: null,
  };
  const ld = LS.get(draftKey(d), null);
  if (ld && ld.savedAt > d.baseU) { Object.assign(d, ld.d, { dirty: true, restored: true }); }
  return d;
}
function guardarRascunho() {
  const d = S.draft; if (!d) return;
  d.dirty = true;
  LS.set(draftKey(d), { savedAt: Date.now(), d: { s: d.s, fText: d.fText, a: d.a, o: d.o } });
}
function entradaDoDraft(d) {
  const f = Calc.parseNum(d.fText, { integer: true });
  return { c: true, s: d.s, f: f.ok ? f.value : null,
    a: d.a.map((g) => { const b = parsePeso(g.bText); return { i: g.i, b: b.ok ? b.value : null, t: g.t }; }) };
}
function itemAtual() {
  const ctx = ctxAtual(); if (!ctx) return null;
  return ctx.itens(R.amb).find((x) => x.key === R.key) || null;
}

VIEWS.item = (r) => {
  const h = contagem(r.cid);
  if (!h) return topbar({ back: '#/', title: 'Contagem não encontrada' });
  if (fechadaStatus(h.status)) { AFTER.push(() => route().name === R.name && go(`#/c/${h.id}/resumo`)); return ''; }
  if (!S.cur.lancOk[r.amb]) return `${topbar({ back: `#/c/${r.cid}/a/${r.amb}`, eyebrow: esc(ambNome(r.amb, h)), title: 'Carregando…' })}`;
  const it = itemAtual();
  if (!it) return `${topbar({ back: `#/c/${r.cid}/a/${r.amb}`, eyebrow: esc(ambNome(r.amb, h)), title: 'Item não encontrado' })}`;
  if (!S.draft || S.draft.key !== r.key || S.draft.amb !== r.amb || S.draft.cid !== r.cid) S.draft = novoDraft(r, it);
  const d = S.draft, E = embL(it.embalagem);
  const e = it.e;
  const p = it.p;
  const temTara = isNum(p.taraG);
  const hist = (e && e.h) || [];
  const fechLbl = it.forma === 'peso' ? '' : E.f;
  const abLbl = it.forma === 'peso' ? 'Pesagens' : E.ab;
  const addLbl = it.forma === 'peso' ? 'Adicionar pesagem' : E.add;

  let convHint = '';
  if (it.unidade === 'LITRO') convHint = isNum(p.volumeMl) ? `Cada ${E.s}: ${F(p.volumeMl)} ml` : 'Volume nominal não cadastrado — litros das fechadas ficarão pendentes';
  if (it.unidade === 'KG') convHint = isNum(p.pesoLiqG) ? `Cada ${E.s}: ${F(p.pesoLiqG)} g` : `Peso por ${E.s} não cadastrado — os kg das fechadas ficam pendentes`;
  if (it.unidade === 'UNID') convHint = 'Controle por unidade';

  return `${topbar({ back: `#/c/${r.cid}/a/${r.amb}`, eyebrow: esc(ambNome(r.amb, h)),
    title: `${esc(it.descricao)}${it.varNome ? ` <span class="muted">· ${esc(it.varNome)}</span>` : ''}`,
    extra: `<div class="small muted" style="margin-top:4px">${esc(it.codigo)} · ${esc(it.categoria)} · <b>${it.unidade}</b> · ${esc(FORMA_LABEL[it.forma] || '')}</div>` })}
  ${(() => { const hp = ctxParDe(h); if (!hp || !it.e) return ''; const vp = valorPar(hp, r.amb, it.key); return vp.estado === 'ok' ? `<div class="note n-info small" style="margin-top:10px">Domingo (${esc(rotuloC(hp.h))}): <b>${qtdTxt(vp.v, it.unidade)}</b> neste ambiente.</div>` : ''; })()}
  <div class="stack" style="margin-top:14px">
    ${d.restored ? '<div class="note n-info">Rascunho não salvo recuperado deste aparelho. Confira e salve.</div>' : ''}
    ${d.remote ? `<div class="note n-warn">Este item foi alterado por ${esc(d.remote.p || 'outra pessoa')} às ${horaBR(d.remote.u)}. Seus dados não salvos foram mantidos na tela; ao salvar, eles substituem o outro lançamento.</div>` : ''}
    <div class="row"><button class="qthumb" style="width:56px;height:56px;border-radius:12px" data-act="fotoItem" data-key="${esc(it.key)}" aria-label="Foto do produto">${miniHTML(it)}</button>
      ${it.obsProd ? `<div class="small muted grow">${esc(it.obsProd)}</div>` : '<span class="small muted grow">Toque na imagem para tirar ou trocar a foto.</span>'}</div>
    ${e ? `<div class="row small"><span class="pill ${e.s ? 'p-zero' : 'p-ok'}">${e.s ? 'Sem estoque' : 'Contado'}</span><span class="muted">por ${esc(e.p || '—')} · ${horaBR(e.u)}</span></div>` : '<div class="row small"><span class="pill p-pend">Não contado</span></div>'}

    <button class="btn big full ${d.s ? 'warn' : ''}" data-act="semEstoque" aria-pressed="${d.s}">${d.s ? '✓ Sem estoque (quantidade zero) — toque para desfazer' : 'Sem estoque'}</button>

    ${d.s ? '<div class="note n-info">Será registrado como contado com quantidade zero neste ambiente.</div>' : `
    ${it.forma !== 'peso' ? `<section class="card stack">
      <div class="row between"><h3>${fechLbl}</h3><span class="small muted">unidades inteiras</span></div>
      <div class="stepper">
        <button data-act="step" data-d="-1" aria-label="Diminuir">−</button>
        <input id="f-qtd" inputmode="numeric" pattern="[0-9]*" data-in="fqtd" value="${esc(d.fText)}" placeholder="—" aria-label="${fechLbl}" autocomplete="off">
        <button data-act="step" data-d="1" aria-label="Aumentar">+</button>
      </div>
      <div id="f-msg" class="small muted">${esc(convHint)}</div>
    </section>` : ''}
    ${it.forma !== 'fechadas' ? `<section class="card stack">
      <div class="row between"><h3>${abLbl}</h3><span class="small muted">${d.a.length} ${d.a.length === 1 ? 'lançada' : 'lançadas'}</span></div>
      <div class="note ${temTara ? 'n-info' : 'n-warn'} small">
        Digite o peso da balança <b>em kg</b> (ex.: 2,350) e escolha o recipiente de cada pesagem — a tara dele é descontada.
        ${temTara ? `Recipiente padrão deste item: <b>${F(p.taraG)} g</b>.` : 'Este item não tem recipiente padrão: escolha em cada pesagem.'}
      </div>
      ${ehGestor() ? `<button class="btn ghost small full" style="min-height:44px" data-act="salvarTara" data-key="${esc(it.key)}">${temTara ? `Mudar a tara padrão (hoje ${F(p.taraG)} g)` : 'Definir a tara padrão deste item'}</button>` : ''}
      <div id="gl" class="stack">${d.a.map((g, ix) => garrafaHTML(g, ix, it)).join('')}</div>
      <button class="btn big full" data-act="addG">+ ${addLbl}</button>
    </section>` : ''}`}

    <label class="f">Observação do lançamento<textarea id="obs" data-in="obs" placeholder="Opcional">${esc(d.o)}</textarea></label>
    <div id="calc">${calcHTML(it)}</div>
    ${hist.length ? `<details class="small"><summary class="muted">Histórico de alterações (${hist.length})</summary>
      <div class="stack" style="margin-top:8px">${hist.slice().reverse().map((x) => `<div>${horaBR(x.t)} · <b>${esc(x.p || '—')}</b> — ${esc(x.d)}</div>`).join('')}</div></details>` : ''}
    ${e ? '<button class="btn ghost dan" data-act="limparItem">Apagar lançamento (voltar para não contado)</button>' : ''}
  </div>
  <div class="bar"><div class="in">
    <button class="btn big" data-act="salvar" data-next="0" id="b-salvar">Salvar e voltar</button>
    <button class="btn big pri" data-act="salvar" data-next="1" id="b-prox">Salvar e próximo</button>
  </div></div>`;
};

function garrafaHTML(g, ix, it) {
  const E = embL(it.embalagem);
  const nome = `Pesagem ${ix + 1}`;
  return `<div class="garrafa" id="g-${g.i}">
    <div class="gh"><b>${nome}</b><button class="btn ghost small" style="min-height:40px" data-act="delG" data-i="${g.i}" aria-label="Excluir ${nome}">Excluir</button></div>
    <div class="grid2" style="margin-top:6px">
      <label class="f">Peso da balança<div class="unitwrap"><input id="gb-${g.i}" inputmode="decimal" data-in="gb" data-i="${g.i}" value="${esc(g.bText)}" placeholder="ex.: 2,350" autocomplete="off"><span class="u">kg</span></div></label>
      <label class="f">Recipiente${recipSelect(g, it, `data-ch="gt" data-i="${g.i}" id="gt-${g.i}"`)}</label>
    </div>
    <div id="gr-${g.i}">${garrafaRes(g, it)}</div></div>`;
}
function garrafaRes(g, it) {
  const pb = parsePeso(g.bText);
  if (!pb.ok) return `<div class="note n-err small" style="margin-top:6px">${esc(pb.error)}</div>`;
  if (pb.empty) return `<div class="small muted" style="margin-top:6px">Tara: ${isNum(g.t) ? F(g.t) + ' g' : 'escolha o recipiente'} · digite o peso para calcular</div>`;
  const a = Calc.aberta({ b: pb.value, t: g.t }, it.p, 'KG');
  const kv = [['Balança', `${FA(a.brutoG / 1000, 3)} kg`], ['Tara', isNum(a.taraG) ? `${F(a.taraG)} g` : '—'],
    ['Líquido', isNum(a.kg) ? `${FA(a.kg, 3)} kg` : '—']];
  let notes = '';
  if (a.erro) notes += `<div class="note n-err small">${esc(a.erro)}</div>`;
  if (!isNum(g.t)) notes += '<div class="note n-warn small">Escolha o recipiente para descontar a tara.</div>';
  a.alertas.forEach((x) => { notes += `<div class="note n-warn small">⚠ ${esc(x)}</div>`; });
  return `<div class="kv">${kv.map(([k, v]) => `<div><span class="k">${k}</span><span class="v">${v}</span></div>`).join('')}</div>${notes ? `<div class="stack" style="gap:6px;margin-top:8px">${notes}</div>` : ''}`;
}
function calcHTML(it) {
  const d = S.draft;
  const rr = Calc.item(entradaDoDraft(d), it.p, it.unidade, it.forma);
  const cells = [];
  const v = (x, dec = 3, suf = '') => (isNum(x) ? `${FA(x, dec)}${suf}` : null);
  const cell = (k, val, pendTxt = 'pendente') => `<div><div class="k">${k}</div><div class="v ${val === null ? 'pend' : ''}">${val === null ? pendTxt : val}</div></div>`;
  if (d.s) return `<div class="result"><div class="big"><div class="k">Total neste ambiente</div><div class="v">0 ${rr.totalRotulo}</div></div></div>`;
  const E2 = embL(it.embalagem);
  if (it.forma !== 'peso') cells.push(cell(it.unidade === 'UNID' ? 'Unidades' : E2.f, rr.fechadas === null ? '—' : `${rr.fechadas}`));
  if (it.unidade === 'LITRO' && it.forma !== 'peso') cells.push(cell('Litros das fechadas', v(rr.fechadasL, 3, ' L'), rr.fechadas === null ? '—' : 'pendente'));
  if (it.unidade === 'KG' && it.forma !== 'peso') cells.push(cell(`Kg das ${E2.pl} fechadas`, v(rr.fechadasKg, 3, ' kg'), rr.fechadas === null ? '—' : 'pendente'));
  if (it.forma !== 'fechadas') {
    cells.push(cell('Pesagens', `${rr.nAbertas}`));
    if (it.unidade === 'KG') cells.push(cell('Kg pesados (líquido)', rr.nAbertas ? v(rr.kgAbertas, 3, ' kg') : '0 kg'));
    else cells.push(cell('Líquido nas abertas', rr.nAbertas ? (isNum(rr.volMl) ? `${F(rr.volMl)} ml` : null) : '0 ml'));
    if (it.unidade === 'UNID' && rr.nAbertas) cells.push(cell('Equivalente em unidades', isNum(rr.equivUnid) ? `${FA(rr.equivUnid, 2)} un. (estimativa)` : null));
  }
  let big = '';
  if (it.unidade === 'UNID') big = `<div class="big"><div class="k">Total neste ambiente</div><div class="v">${rr.totalOk ? `${F(rr.total)} un.` : '—'}</div></div>`;
  else big = `<div class="big"><div class="k">Total neste ambiente (${it.unidade})</div><div class="v ${rr.totalOk ? '' : 'pend'}">${rr.totalOk ? `${FA(rr.total, 3)} ${rr.totalRotulo}` : 'Cálculo pendente — sem total'}</div></div>`;
  const pend = rr.pendencias.filter((x) => x !== 'Não contado');
  return `<div class="result">${cells.join('')}${big}</div>
    ${pend.length ? `<div class="note n-warn small" style="margin-top:8px">${pend.map(esc).join('<br>')}</div>` : ''}`;
}
function atualizarCalc() {
  const it = itemAtual(); if (!it) return;
  const c = $('#calc'); if (c) c.innerHTML = calcHTML(it);
}
function atualizarItemRemoto() {
  paintSync();
  const it = itemAtual();
  if (!it || !S.draft) { render(); return; }
  const e = it.e, u = (e && e.u) || 0;
  if (u !== S.draft.baseU && !S.saving) {
    if (!S.draft.dirty) S.draft = null;
    else if (e && e.p !== S.me) S.draft.remote = e;
    else S.draft.baseU = u;
  }
  render();
}

INP.fqtd = (el) => {
  S.draft.fText = el.value.trim(); guardarRascunho();
  const pf = Calc.parseNum(S.draft.fText, { integer: true });
  el.classList.toggle('bad', !pf.ok);
  const m = $('#f-msg'); if (m && !pf.ok) { m.textContent = pf.error; m.className = 'small'; m.style.color = 'var(--danger)'; }
  else if (m) { m.style.color = ''; m.className = 'small muted'; }
  atualizarCalc();
};
ACT.step = (el) => {
  const d = S.draft; const pf = Calc.parseNum(d.fText, { integer: true });
  const cur = pf.ok && pf.value !== null ? pf.value : 0;
  const nv = Math.max(0, cur + Number(el.dataset.d));
  d.fText = String(pf.ok && pf.value === null && el.dataset.d === '-1' ? 0 : nv);
  guardarRascunho();
  const inp = $('#f-qtd'); if (inp) { inp.value = d.fText; inp.classList.remove('bad'); }
  atualizarCalc();
};
ACT.semEstoque = async () => {
  const d = S.draft;
  if (!d.s) {
    const temDados = d.a.length || (d.fText && d.fText !== '0');
    if (temDados && !(await confirmar('Marcar como sem estoque?', 'As quantidades e pesagens digitadas neste item serão descartadas.', 'Marcar sem estoque'))) return;
    d.s = true; d.a = []; d.fText = '';
  } else d.s = false;
  guardarRascunho(); render();
};
ACT.addG = () => {
  const it = itemAtual(); const ult = S.draft.a[S.draft.a.length - 1];
  const g = { i: uid('g'), bText: '', t: ult && isNum(ult.t) ? ult.t : (isNum(it.p.taraG) ? it.p.taraG : null), reg: null };
  S.draft.a.push(g); guardarRascunho();
  AFTER.push(() => { const el = $(`#gb-${g.i}`); if (el) { el.focus(); el.scrollIntoView({ block: 'center' }); } });
  render();
};
ACT.delG = async (el) => {
  const d = S.draft; const ix = d.a.findIndex((g) => g.i === el.dataset.i);
  if (ix < 0) return;
  const g = d.a[ix];
  if (g.bText && !(await confirmar('Excluir pesagem?', `Remover a pesagem ${ix + 1} (${esc(g.bText)} kg)? A alteração vale ao salvar o item.`, 'Excluir', 'dan'))) return;
  d.a.splice(ix, 1); guardarRascunho(); render();
};
INP.gb = (el) => {
  const it = itemAtual(); const g = S.draft.a.find((x) => x.i === el.dataset.i); if (!g) return;
  g.bText = el.value.trim(); guardarRascunho();
  const pb = parsePeso(g.bText);
  el.classList.toggle('bad', !pb.ok);
  const box = $(`#gr-${g.i}`); if (box) box.innerHTML = garrafaRes(g, it);
  atualizarCalc();
};
INP.gt = (el) => {
  const it = itemAtual(); const g = S.draft.a.find((x) => x.i === el.dataset.i); if (!g) return;
  g.t = el.value === '' ? null : Number(el.value); guardarRascunho();
  const box = $(`#gr-${g.i}`); if (box) box.innerHTML = garrafaRes(g, it);
  atualizarCalc();
};
INP.obs = (el) => { S.draft.o = el.value; guardarRascunho(); };

function resumoLanc(en, it) {
  if (en.s) return 'sem estoque';
  const p = [];
  if (it.forma !== 'peso') p.push(`${en.f} ${embL(it.embalagem).pl}`);
  if (en.a.length) p.push(`${en.a.length} pesagem(ns) (${en.a.map((g) => FA(g.b / 1000, 3) + ' kg').join(', ')})`);
  return p.join(' + ') || '—';
}
ACT.salvar = async (el) => {
  if (S.saving) return;
  const next = el.dataset.next === '1';
  const d = S.draft, it = itemAtual(); if (!d || !it) return;
  const E = embL(it.embalagem); const nm = 'pesagem';
  const errs = []; let f = null; const a = [];
  if (!d.s && it.forma !== 'peso') {
    const pf = Calc.parseNum(d.fText, { integer: true });
    if (pf.empty) errs.push(`Informe a quantidade de ${it.unidade === 'UNID' ? 'unidades' : E.f.toLowerCase()} (use 0 se não houver) ou marque Sem estoque.`);
    else if (!pf.ok) errs.push(`${E.f}: ${pf.error}`);
    else f = pf.value;
  }
  if (!d.s && it.forma !== 'fechadas') {
    d.a.forEach((g, ix) => {
      const pb = parsePeso(g.bText);
      if (pb.empty) errs.push(`Informe o peso da ${nm} ${ix + 1} ou exclua-a.`);
      else if (!pb.ok) errs.push(`${cap(nm)} ${ix + 1}: ${pb.error}`);
      else a.push({ i: g.i, b: pb.value, t: isNum(g.t) ? g.t : null, r: g.reg || Date.now() });
    });
    if (it.forma === 'peso' && !d.a.length) errs.push('Adicione pelo menos uma pesagem ou marque Sem estoque.');
  }
  if (errs.length) { await modal({ title: 'Falta completar', body: `<div class="note n-err">${errs.map(esc).join('<br>')}</div>`, actions: [{ label: 'Voltar ao lançamento', value: true, cls: 'pri' }] }); return; }
  const now = Date.now();
  const entry = { c: true, s: !!d.s, f: d.s ? 0 : f, a: d.s ? [] : a, o: d.o.trim(), u: now, p: S.me || '' };
  const rr = Calc.item(entry, it.p, it.unidade, it.forma);
  if (rr.erros.length) { await modal({ title: 'Pesagem inválida', body: `<div class="note n-err">${[...new Set(rr.erros)].map(esc).join('<br>')}</div>`, actions: [{ label: 'Revisar lançamento', value: true, cls: 'pri' }] }); return; }
  if (rr.alertas.length) {
    const ok = await modal({ title: 'Confira antes de salvar', body: `<div class="note n-warn">${[...new Set(rr.alertas)].map(esc).join('<br>')}</div><p class="small muted">Pode ser erro de digitação na pesagem ou referência errada no cadastro (tara ou peso da porção).</p>`,
      actions: [{ label: 'Revisar lançamento', value: false, cls: 'pri' }, { label: 'Salvar mesmo assim', value: true }] });
    if (!ok) return;
  }
  const old = it.e;
  entry.h = [...((old && old.h) || []), { t: now, p: entry.p, d: `${old ? 'Alterado' : 'Lançado'}: ${resumoLanc(entry, it)}` }].slice(-4);
  S.saving = true; $$('#b-salvar,#b-prox').forEach((b) => { b.disabled = true; }); $('#b-prox') && ($('#b-prox').textContent = 'Salvando…');
  const path = `contagens/${d.cid}/lanc/${d.amb}`;
  try {
    await comSync(async () => {
      const ref = S.db.doc(path);
      try { await ref.update({ i: { [d.key]: entry } }); }
      catch (e) {
        if (e && e.code === 'invalid_argument') { const sn = await ref.get(); if (!sn.exists) { await ref.set({ i: { [d.key]: entry } }); return; } }
        throw e;
      }
    });
    LS.del(draftKey(d));
    S.saving = false;
    toast('✓ Salvo');
    if (next) irProximo(d.key);
    else voltarParaLista(d.key);
  } catch (e) {
    S.saving = false;
    $$('#b-salvar,#b-prox').forEach((b) => { b.disabled = false; }); $('#b-prox') && ($('#b-prox').textContent = 'Salvar e próximo');
    toast(errMsg(e) + ' O rascunho ficou guardado neste aparelho.', 5000);
  }
};
function voltarParaLista(atual) {
  const ordem = (S.ui.listaOrdem || []);
  const ix = ordem.indexOf(atual);
  S.ui.rolarPara = ix >= 0 ? ordem.slice(ix) : [atual];
  S.draft = null;
  go(`#/c/${R.cid}/a/${R.amb}`);
}
function irProximo(atual) {
  const lista = ctxAtual().itens(R.amb).filter((x) => !S.ui.cat || x.categoria === S.ui.cat);
  const ix = lista.findIndex((x) => x.key === atual);
  const ordem = [...lista.slice(ix + 1), ...lista.slice(0, Math.max(ix, 0))];
  const prox = ordem.find((x) => x.key !== atual && x.r.status === 'nao_contado');
  S.draft = null;
  if (!prox) { toast('Todos os itens deste ambiente foram contados ✓', 3000); go(`#/c/${R.cid}/a/${R.amb}`); return; }
  go(`#/c/${R.cid}/a/${R.amb}/i/${encodeURIComponent(prox.key)}`);
}
ACT.limparItem = async () => {
  const d = S.draft;
  if (!(await confirmar('Apagar lançamento?', 'O item volta a ficar como <b>não contado</b> neste ambiente.', 'Apagar', 'dan'))) return;
  try {
    await comSync(() => S.db.doc(`contagens/${d.cid}/lanc/${d.amb}`).update({ i: { [d.key]: null } }));
    LS.del(draftKey(d)); S.draft = null; toast('Lançamento apagado'); render();
  } catch (e) { toast(errMsg(e), 4000); }
};

/* ---- Cadastrar/atualizar a tara direto da pesagem ---- */
ACT.salvarTara = async (el) => {
  const key = el.dataset.key || '';
  const codigo = key.split('~')[0], vid = key.split('~')[1];
  const prod = S.produtos.get(codigo);
  if (!prod) return toast('Produto não encontrado no cadastro');
  const lista = prod.variantes || [];
  const v = lista.find((x) => x.id === vid) || lista[0];
  if (!v) return toast('Variante não encontrada');
  const atual = isNum(v.taraG) ? v.taraG : null;
  const txt = await modal({
    title: atual === null ? 'Cadastrar a tara' : 'Atualizar a tara',
    body: `<p class="small muted">${esc(prod.descricao)}${v.nome && v.nome !== 'Padrão' ? ' · ' + esc(v.nome) : ''}</p>
      <label class="f">Peso do recipiente vazio<div class="unitwrap"><input id="m-tara" inputmode="decimal" value="${atual === null ? '' : esc(String(atual).replace('.', ','))}" placeholder="ex.: 230 (0 = sem recipiente)" autocomplete="off"><span class="u">g</span></div></label>
      <p class="small muted">Em gramas, com tampa se vocês pesam com tampa. Use 0 se o item é pesado direto (saco a vácuo, pacote). Vale para este produto em todos os ambientes; contagens já encerradas não mudam.</p>`,
    actions: [
      { label: 'Salvar tara', cls: 'pri',
        check: (m) => { const r = Calc.parseNum(m.querySelector('#m-tara').value, { thousandsDot: true });
          if (!r.ok) { toast(r.error); return false; }
          if (r.empty) { toast('Digite o peso do recipiente vazio (0 se não usa)'); return false; }
          return true; },
        collect: (m) => m.querySelector('#m-tara').value },
      { label: 'Cancelar', value: null, cls: 'ghost' }],
  });
  if (!txt) return;
  const g = Calc.parseNum(txt, { thousandsDot: true }).value;
  const variantes = lista.map((x) => (x.id === v.id ? { ...x, taraG: g } : x));
  try {
    await comSync(() => S.db.doc(`produtos/${codigo}`).update({ variantes, atualizadoEm: Date.now(), atualizadoPor: S.me || '' }));
    toast(`Tara de ${prod.descricao}: ${F(g)} g ✓`, 3000);
    render();
  } catch (e) { toast(errMsg(e), 4000); }
};

/* ===== Revisão, finalização, resumo ===== */
function pendenciasCtx(ctx) {
  const out = { nc: 0, conv: 0, err: 0, alert: 0, porAmb: {} };
  for (const a of ctx.ambs) {
    const lista = ctx.itens(a.id);
    const nc = lista.filter((x) => x.r.status === 'nao_contado');
    const conv = lista.filter((x) => x.r.status !== 'nao_contado' && x.r.pendencias.length);
    const al = lista.filter((x) => x.r.alertas.length || x.r.erros.length);
    out.porAmb[a.id] = { lista, nc, conv, al, pr: progresso(lista) };
    out.nc += nc.length; out.conv += conv.length; out.alert += al.length;
  }
  return out;
}
function linkItem(ctx, ambId, it, extra = '') {
  return `<button class="li" data-act="go" data-to="#/c/${ctx.h.id}/a/${ambId}/i/${encodeURIComponent(it.key)}">
    <span class="grow"><span class="nm">${esc(it.descricao)}${it.varNome ? ` · ${esc(it.varNome)}` : ''}</span><br><span class="meta">${esc(it.codigo)} · ${it.unidade}${extra ? ' · ' + extra : ''}</span></span><span class="muted">›</span></button>`;
}

VIEWS.revisao = (r) => {
  const h = contagem(r.cid);
  if (!h) return topbar({ back: '#/', title: 'Contagem não encontrada' });
  if (fechadaStatus(h.status)) { AFTER.push(() => route().name === R.name && go(`#/c/${h.id}/resumo`)); return ''; }
  if ((h.ambientes || []).some((a) => !S.cur.lancOk[a])) return `${topbar({ back: `#/c/${h.id}`, title: 'Carregando…' })}`;
  const ctx = ctxAtual();
  const P = pendenciasCtx(ctx);
  const tudoOk = P.nc === 0 && P.conv === 0;
  return `${topbar({ back: `#/c/${h.id}`, eyebrow: `Contagem de ${rotuloC(h)}`, title: 'Revisar contagem' })}
  <div class="stack" style="margin-top:14px">
    <div class="grid2">
      <div class="card"><div class="eyebrow">Não contados</div><div class="num" style="font-size:28px;font-weight:700;font-family:var(--f-head)">${P.nc}</div></div>
      <div class="card"><div class="eyebrow">Cálculo pendente</div><div class="num" style="font-size:28px;font-weight:700;font-family:var(--f-head)">${P.conv}</div></div>
    </div>
    ${ctx.ambs.map((a) => {
      const x = P.porAmb[a.id];
      return `<section class="card stack">
        <div class="row between"><h2>${esc(a.nome)}</h2><span class="num muted">${x.pr.contados}/${x.pr.total}</span></div>
        <div class="prog"><i style="width:${x.pr.pct}%"></i></div>
        ${x.nc.length ? `<details><summary><b>${x.nc.length} não contados</b></summary><div class="list" style="margin-top:8px">${x.nc.map((it) => linkItem(ctx, a.id, it)).join('')}</div></details>` : '<div class="note n-ok small">Todos os itens contados.</div>'}
        ${x.conv.length ? `<details><summary><b>${x.conv.length} com cálculo pendente</b> <span class="muted small">(falta recipiente/tara ou peso da porção)</span></summary><div class="list" style="margin-top:8px">${x.conv.map((it) => linkItem(ctx, a.id, it, esc(it.r.pendencias.join('; ')))).join('')}</div></details>` : ''}
        ${x.al.length ? `<details><summary><b>${x.al.length} com alerta</b></summary><div class="list" style="margin-top:8px">${x.al.map((it) => linkItem(ctx, a.id, it, esc([...it.r.erros, ...it.r.alertas][0] || ''))).join('')}</div></details>` : ''}
      </section>`;
    }).join('')}
    <button class="btn full" data-act="go" data-to="#/c/${h.id}/resumo">Ver prévia do resumo</button>
    <section class="card stack lift">
      <h2>Finalizar</h2>
      <p class="small muted" style="margin:0">Ao finalizar, a contagem fica travada e os parâmetros do cadastro (taras e pesos de porção) usados nos cálculos são guardados junto com ela. Alterações futuras no cadastro não mudam esta contagem.</p>
      ${!ehGestor() ? '<div class="note n-info">Quem finaliza a contagem é o gestor.</div>' : tudoOk ? `<button class="btn big pri full" data-act="finalizar" data-parcial="0">Finalizar contagem</button>` : `
        <div class="note n-warn">Há pendências: <b>${P.nc}</b> itens não contados e <b>${P.conv}</b> com cálculo pendente. Eles aparecerão como pendentes no resumo e no Excel (sem total inventado).</div>
        <label class="check"><input type="checkbox" id="fin-ok" data-ch="finOk"> Confirmo o encerramento parcial com ${P.nc} não contados e ${P.conv} cálculos pendentes</label>
        <label class="f">Observação do encerramento<textarea id="fin-obs" placeholder="Ex.: Estoque não contado por falta de acesso"></textarea></label>
        <button class="btn big warn full" data-act="finalizar" data-parcial="1" id="btn-fin" disabled>Encerrar com pendências</button>`}
    </section>
  </div>`;
};
INP.finOk = (el) => { const b = $('#btn-fin'); if (b) b.disabled = !el.checked; };

ACT.finalizar = async (el) => {
  if (!ehGestor()) return toast('Só o gestor finaliza a contagem');
  if (S.saving) return;
  const parcial = el.dataset.parcial === '1';
  if (parcial && !($('#fin-ok') && $('#fin-ok').checked)) return toast('Marque a confirmação das pendências');
  const h = contagem(S.cur.cid); const ctx = ctxAtual(); const P = pendenciasCtx(ctx);
  if (!parcial && (P.nc || P.conv)) return render();
  const obs = ($('#fin-obs') && $('#fin-obs').value.trim()) || '';
  // monta os parâmetros congelados
  const snap = { v: {}, amb: {}, criadoEm: Date.now(), por: S.me || '' };
  for (const a of ctx.ambs) {
    const lista = ctx.itens(a.id);
    snap.amb[a.id] = { nome: a.nome, itens: lista.map((it) => ({ k: it.key, forma: it.forma })) };
    for (const it of lista) {
      if (!snap.v[it.key]) snap.v[it.key] = { codigo: it.codigo, descricao: it.descricao, varNome: it.varNome || '', varNomeFull: it.varNomeFull || it.varNome || '',
        categoria: it.categoria, unidade: it.unidade, formaProd: it.formaProd || it.forma, embalagem: it.embalagem, params: it.p };
    }
  }
  S.saving = true; el.disabled = true; el.textContent = 'Finalizando…';
  try {
    await comSync(async () => {
      await S.db.doc(`contagens/${h.id}/snap/params`).set(snap);
      await S.db.doc(`contagens/${h.id}`).update({
        status: parcial ? 'parcial' : 'finalizada', finalizadoEm: Date.now(), finalizadoPor: S.me || '', obsEnc: obs,
        resumoEnc: { naoContados: P.nc, calcPendentes: P.conv, alertas: P.alert,
          porAmb: Object.fromEntries(ctx.ambs.map((a) => [a.id, { contados: P.porAmb[a.id].pr.contados, total: P.porAmb[a.id].pr.total, nc: P.porAmb[a.id].nc.length, conv: P.porAmb[a.id].conv.length }])) },
        eventos: [...(h.eventos || []), { t: Date.now(), p: S.me || '', d: parcial ? `Encerrada com pendências (${P.nc} não contados, ${P.conv} cálculos pendentes)` : 'Finalizada' }].slice(-50),
        atualizadoEm: Date.now(),
      });
    });
    toast(parcial ? 'Contagem encerrada com pendências' : 'Contagem finalizada ✓', 3000);
    delete S.cache[h.id];
    if (h.tipo === 'segunda' && h.par && contagem(h.par)) { S.ui.cmpFiltro = 'div'; S.ui.cmpAmb = 'cons'; go(`#/comparar/${h.par}/${h.id}`); }
    else go(`#/c/${h.id}/resumo`);
  } catch (e) { toast(errMsg(e), 4000); el.disabled = false; el.textContent = 'Tentar novamente'; }
  finally { S.saving = false; }
};

/* ---- Finalizar com um botão: fecha e já gera a planilha ---- */
ACT.finalizarRapido = async (el) => {
  if (!ehGestor()) return toast('Só o gestor finaliza a contagem');
  if (S.saving) return;
  const h = contagem(S.cur.cid); const ctx = ctxAtual(); if (!h || !ctx) return;
  if ((h.ambientes || []).some((a) => !S.cur.lancOk[a])) return toast('Carregando lançamentos… tente de novo');
  const P = pendenciasCtx(ctx);
  const parcial = !!(P.nc || P.conv);
  const ok = await modal({ title: 'Finalizar contagem?',
    body: parcial ? `${P.nc ? `<p><b>${P.nc}</b> ${P.nc === 1 ? 'item ficou' : 'itens ficaram'} em branco. Na planilha eles ficam vazios no ambiente (= não tinha lá), como na planilha de papel.</p>` : ''}
      ${P.conv ? `<p><b>${P.conv}</b> ${P.conv === 1 ? 'item está' : 'itens estão'} sem recipiente ou peso da porção e ${P.conv === 1 ? 'sai' : 'saem'} como “pendente”.</p>` : ''}
      <label class="check"><input type="checkbox" id="m-ok"> Finalizar mesmo assim</label>`
      : '<p>Tudo contado. A contagem fica travada e a planilha Excel é gerada em seguida.</p>',
    actions: [{ label: 'Finalizar e gerar planilha', cls: 'pri', value: true, check: (m) => !parcial || m.querySelector('#m-ok').checked || (toast('Marque a confirmação'), false) },
      { label: 'Cancelar', value: false, cls: 'ghost' }] });
  if (!ok) return;
  const snap = { v: {}, amb: {}, criadoEm: Date.now(), por: S.me || '' };
  for (const a of ctx.ambs) {
    const lista = ctx.itens(a.id);
    snap.amb[a.id] = { nome: a.nome, itens: lista.map((it) => ({ k: it.key, forma: it.forma })) };
    for (const it of lista) {
      if (!snap.v[it.key]) snap.v[it.key] = { codigo: it.codigo, descricao: it.descricao, varNome: it.varNome || '', varNomeFull: it.varNomeFull || it.varNome || '',
        categoria: it.categoria, unidade: it.unidade, formaProd: it.formaProd || it.forma, embalagem: it.embalagem, params: it.p };
    }
  }
  S.saving = true; S.ui.finalizando = true; render();
  const lancCopia = JSON.parse(JSON.stringify(S.cur.lanc));
  try {
    await comSync(async () => {
      await S.db.doc(`contagens/${h.id}/snap/params`).set(snap);
      await S.db.doc(`contagens/${h.id}`).update({
        status: parcial ? 'parcial' : 'finalizada', finalizadoEm: Date.now(), finalizadoPor: S.me || '', obsEnc: '',
        resumoEnc: { naoContados: P.nc, calcPendentes: P.conv, alertas: P.alert,
          porAmb: Object.fromEntries(ctx.ambs.map((a) => [a.id, { contados: P.porAmb[a.id].pr.contados, total: P.porAmb[a.id].pr.total, nc: P.porAmb[a.id].nc.length, conv: P.porAmb[a.id].conv.length }])) },
        eventos: [...(h.eventos || []), { t: Date.now(), p: S.me || '', d: parcial ? `Encerrada com pendências (${P.nc} não contados, ${P.conv} cálculos pendentes)` : 'Finalizada' }].slice(-50),
        atualizadoEm: Date.now(),
      });
    });
  } catch (e) { toast(errMsg(e), 4000); S.saving = false; S.ui.finalizando = false; render(); return; }
  S.saving = false;
  const ctxFim = ctxDe({ ...h, status: parcial ? 'parcial' : 'finalizada' }, lancCopia, snap);
  let msgDrive = '';
  if (driveAtivo()) try {
    const rd = await enviarContagemDrive(ctxFim, S.user.nome);
    msgDrive = rd.ok ? 'Planilha enviada ao Drive ✓' : rd.fila ? `Drive: ficou na fila (${rd.msg})` : `Drive: ${rd.msg}`;
  } catch (e) { msgDrive = 'Drive: não foi possível enviar — tente pelo Histórico'; }
  if (msgDrive) toast(msgDrive, 5000);
  try {
    if (!S.dl) throw new Error('download indisponível nesta visualização');
    await carregarLib('ExcelJS');
    const { wb, nome } = await gerarWorkbook(ctxFim, 'todos');
    const buf = await wb.xlsx.writeBuffer();
    await S.dl.save({ filename: nome, data: new Blob([buf], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }) });
    toast(msgDrive ? msgDrive + ' · planilha baixada' : 'Planilha baixada ✓', 5000);
  } catch (e) {
    if (e && e.code === 'declined') toast(msgDrive || 'Download cancelado — gere de novo pelo Histórico', 5000);
    else { console.error(e); toast('Contagem salva, mas a planilha não saiu: ' + ((e && e.message) || e) + '. Gere pelo Histórico.', 6000); }
  }
  S.ui.finalizando = false;
  delete S.cache[h.id];
  if (h.tipo === 'segunda' && h.par && contagem(h.par)) { S.ui.cmpFiltro = 'div'; S.ui.cmpAmb = 'cons'; go(`#/comparar/${h.par}/${h.id}`); }
  else go('#/');
};

/* ---- Reabrir uma contagem encerrada para editar ---- */
ACT.reabrir = async (el) => {
  if (!ehGestor()) return toast('Só o gestor reabre uma contagem');
  const h = contagem(el.dataset.cid);
  if (!h) return;
  const outra = S.contagens.find((c) => c.id !== h.id && !fechadaStatus(c.status));
  if (outra) return toast(`Já existe a contagem de ${dataBR(outra.data)} aberta. Encerre ou exclua ela antes de reabrir esta.`, 5000);
  const ok = await modal({ title: 'Reabrir contagem?',
    body: `<p>A contagem de <b>${dataBR(h.data)}</b> volta para <b>em andamento</b> e pode ser editada de novo. Os lançamentos já feitos continuam lá.</p>
      <p class="small muted">Enquanto estiver aberta, os cálculos voltam a usar o cadastro atual (taras e pesos). Ao encerrar de novo, os parâmetros são congelados outra vez. Se a planilha já foi enviada, gere uma nova depois de fechar.</p>`,
    actions: [{ label: 'Reabrir', value: true, cls: 'pri' }, { label: 'Cancelar', value: false, cls: 'ghost' }] });
  if (!ok) return;
  try {
    await comSync(() => S.db.doc(`contagens/${h.id}`).update({
      status: 'andamento', finalizadoEm: null, finalizadoPor: '', obsEnc: '',
      eventos: [...(h.eventos || []), { t: Date.now(), p: S.me || '', d: 'Contagem reaberta para edição' }].slice(-50),
      atualizadoEm: Date.now(),
    }));
    const loc = S.contagens.find((c) => c.id === h.id);
    if (loc) { loc.status = 'andamento'; loc.finalizadoEm = null; }
    delete S.cache[h.id];  // evita o redirect de volta antes do snapshot chegar
    toast('Contagem reaberta — pode editar');
    go(`#/c/${h.id}`);
  } catch (e) { toast(errMsg(e), 4000); }
};

/* ---- Linhas do resumo (tela e Excel) ---- */
function montarResumo(ctx, ambIds) {
  const map = new Map();
  for (const a of ambIds) {
    for (const it of ctx.itens(a)) {
      if (!map.has(it.key)) map.set(it.key, { key: it.key, info: it, por: {} });
      map.get(it.key).por[a] = it;
    }
  }
  const rows = [...map.values()].sort((x, y) => collator.compare(x.info.categoria, y.info.categoria) ||
    collator.compare(x.info.descricao, y.info.descricao) || collator.compare(x.info.varNomeFull || '', y.info.varNomeFull || ''));
  for (const row of rows) {
    const presentes = ambIds.filter((a) => row.por[a]);
    row.cons = Calc.consolidar(presentes.map((a) => row.por[a].r), row.info.unidade);
    row.naoContadosEm = presentes.filter((a) => row.por[a].r.status === 'nao_contado');
    const pend = [];
    for (const a of presentes) {
      const r = row.por[a].r; const nm = ctx.ambs.find((x) => x.id === a).nome;
      if (r.status === 'nao_contado') pend.push(`${nm}: não contado`);
      else [...r.pendencias, ...r.erros, ...r.alertas].forEach((p) => pend.push(`${nm}: ${p}`));
    }
    row.pend = pend;
    row.obs = presentes.map((a) => row.por[a].e && row.por[a].e.o ? `${ctx.ambs.find((x) => x.id === a).nome}: ${row.por[a].e.o}` : '').filter(Boolean);
  }
  return rows;
}
const sitTxt = (r) => (r.status === 'nao_contado' ? 'NÃO CONTADO' : r.status === 'sem_estoque' ? 'SEM ESTOQUE' : 'CONTADO');
function totalTxt(x, unidade) {
  if (!isNum(x)) return '<span class="pill p-warn">pendente</span>';
  return unidade === 'UNID' ? `${F(x)} un.` : `${FA(x, 3)} ${unidade === 'KG' ? 'kg' : 'L'}`;
}

VIEWS.resumo = (r) => {
  const h = contagem(r.cid);
  if (!h) return topbar({ back: '#/hist', title: 'Contagem não encontrada' });
  const fech = fechadaStatus(h.status);
  if ((h.ambientes || []).some((a) => !S.cur.lancOk[a]) || (fech && S.cur.snap === undefined)) return `${topbar({ back: '#/hist', title: 'Carregando…' })}`;
  const ctx = ctxAtual();
  const ambs = ctx.ambs;
  if (!S.ui.resAmb || !ambs.find((a) => a.id === S.ui.resAmb)) S.ui.resAmb = ambs[0] && ambs[0].id;
  const modo = S.ui.resModo;
  const ids = modo === 'cons' ? ambs.map((a) => a.id) : [S.ui.resAmb];
  const rows = montarResumo(ctx, ids);
  const P = pendenciasCtx(ctx);
  const pill = h.status === 'finalizada' ? 'p-ok' : h.status === 'parcial' ? 'p-warn' : 'p-acc';
  let tabela = '', cat = null;
  for (const row of rows) {
    const it = row.info;
    if (it.categoria !== cat) { cat = it.categoria; tabela += `<tr class="cat"><td colspan="7">${esc(cat)}</td></tr>`; }
    const nome = `<b>${esc(it.descricao)}</b>${it.varNome ? ` · ${esc(it.varNome)}` : ''}<br><span class="muted tiny">${esc(it.codigo)} · ${it.unidade}</span>`;
    if (modo === 'cons') {
      const c = row.cons;
      tabela += `<tr><td>${nome}</td><td class="n">${c.contados && it.formaProd !== 'peso' ? (c.fechadas ?? '—') : '—'}</td><td class="n">${c.contados ? c.nAbertas : '—'}</td>
        <td class="n">${c.contados ? (isNum(c.liqG) ? FA(c.liqG / 1000, 3) : '<span class="pill p-warn">pend.</span>') : '—'}</td>
        <td class="n">${c.contados ? totalTxt(c.total, it.unidade) : '<span class="pill p-pend">não contado</span>'}</td>
        <td class="small">${row.pend.map(esc).join('<br>')}${row.obs.length ? `<br><i>${row.obs.map(esc).join('<br>')}</i>` : ''}</td></tr>`;
    } else {
      const x = row.por[S.ui.resAmb]; const rr = x.r;
      const nc = rr.status === 'nao_contado';
      tabela += `<tr><td>${nome}</td><td><span class="pill ${nc ? 'p-pend' : rr.status === 'sem_estoque' ? 'p-zero' : 'p-ok'}">${sitTxt(rr)}</span></td>
        <td class="n">${nc || x.forma === 'peso' ? '—' : rr.fechadas ?? '—'}</td><td class="n">${nc ? '—' : rr.nAbertas}</td>
        <td class="n">${nc ? '—' : isNum(rr.liqG) ? FA(rr.liqG / 1000, 3) : '<span class="pill p-warn">pend.</span>'}</td>
        <td class="n">${nc ? '—' : totalTxt(rr.totalOk ? rr.total : null, it.unidade)}</td>
        <td class="small">${[...rr.pendencias.filter((p) => p !== 'Não contado'), ...rr.erros, ...rr.alertas].map(esc).join('<br>')}${x.e && x.e.o ? `<br><i>${esc(x.e.o)}</i>` : ''}</td></tr>`;
    }
  }
  const head = modo === 'cons'
    ? '<tr><th>Produto</th><th class="n">Fechadas / unid.</th><th class="n">Pesagens</th><th class="n">Kg pesados</th><th class="n">Total</th><th>Pendências / obs.</th></tr>'
    : '<tr><th>Produto</th><th>Situação</th><th class="n">Fechadas / unid.</th><th class="n">Pesagens</th><th class="n">Kg pesados</th><th class="n">Total</th><th>Pendências / obs.</th></tr>';
  return `${topbar({ back: fech ? '#/hist' : `#/c/${h.id}`, eyebrow: fech ? 'Resumo da contagem' : 'Prévia — contagem em andamento', title: `Contagem de ${rotuloC(h)}` })}
  <div class="stack" style="margin-top:14px">
    <div class="row between" style="gap:10px;flex-wrap:wrap">
      <div class="row small" style="gap:8px;flex-wrap:wrap"><span class="pill ${pill}">${STATUS_LABEL[h.status]}</span><span class="muted">Responsável: <b>${esc(h.responsavel)}</b>${h.finalizadoEm ? ` · encerrada ${horaBR(h.finalizadoEm)}${h.finalizadoPor ? ' por ' + esc(h.finalizadoPor) : ''}` : ''}</span></div>
      ${fech ? `<div class="row" style="gap:8px;flex:0 0 auto">
        ${ehGestor() ? `<button class="btn ghost small" style="min-height:40px" data-act="reabrir" data-cid="${h.id}">Reabrir para editar</button>` : ''}
        <button class="btn dan ghost small" style="min-height:40px" data-act="excluirContagem" data-cid="${h.id}">Excluir contagem</button>
      </div>` : ''}
    </div>
    ${fech && !S.cur.snap ? '<div class="note n-warn small">Parâmetros congelados não encontrados; os cálculos usam o cadastro atual.</div>' : ''}
    ${!fech ? '<div class="note n-info small">Prévia com o cadastro atual. Os valores podem mudar até a finalização.</div>' : ''}
    ${h.obsEnc ? `<div class="note n-info small"><b>Observação do encerramento:</b> ${esc(h.obsEnc)}</div>` : ''}
    ${P.nc || P.conv ? `<div class="note n-warn small">${P.nc} itens não contados · ${P.conv} com cálculo pendente. Nesses itens o total fica em branco.</div>` : ''}
    <section class="card stack lift">
      <h3>Exportar</h3>
      <label class="f">Ambientes<select id="exp-amb" data-ch="expAmb"><option value="todos">Todos os ambientes da contagem</option>${ambs.map((a) => `<option value="${a.id}" ${S.ui.expAmb === a.id ? 'selected' : ''}>${esc(a.nome)}</option>`).join('')}</select></label>
      <button class="btn big pri full" data-act="excel" id="btn-excel">Gerar planilha Excel</button>
      ${fech && h.tipo === 'segunda' && h.par && contagem(h.par) ? `<button class="btn big full" data-act="go" data-to="#/comparar/${h.par}/${h.id}">Ver Domingo × Segunda</button>` : ''}
      ${h.tipo === 'segunda' && h.par ? '<div class="tiny muted">A planilha desta segunda já sai com a aba “Domingo x Segunda”.</div>' : ''}
      ${S.dl === null ? '<div class="small muted">O download de arquivos não está disponível nesta visualização.</div>' : ''}
      ${fech ? cartaoDrive(h) : ''}
    </section>
    <div class="seg"><button data-act="resModo" data-k="amb" class="${modo === 'amb' ? 'on' : ''}">Por ambiente</button><button data-act="resModo" data-k="cons" class="${modo === 'cons' ? 'on' : ''}">Consolidado</button></div>
    ${modo === 'amb' ? `<div class="chips">${ambs.map((a) => `<button class="chip ${S.ui.resAmb === a.id ? 'on' : ''}" data-act="resAmb" data-k="${a.id}">${esc(a.nome)}</button>`).join('')}</div>` : '<p class="small muted" style="margin:0">Soma do mesmo produto nos ambientes desta contagem. Em branco num ambiente = não tinha lá.</p>'}
    <div class="tw"><table><thead>${head}</thead><tbody>${tabela}</tbody></table></div>
  </div>`;
};
ACT.resModo = (el) => { S.ui.resModo = el.dataset.k; render(); };
ACT.resAmb = (el) => { S.ui.resAmb = el.dataset.k; render(); };
INP.expAmb = (el) => { S.ui.expAmb = el.value; };

/* ===== Excel ===== */
ACT.excel = async (el) => {
  if (!S.dl) return toast('Download indisponível nesta visualização.', 4000);
  const ctx = ctxAtual(); const scope = ($('#exp-amb') && $('#exp-amb').value) || 'todos';
  el.disabled = true; el.textContent = 'Preparando…';
  try {
    await carregarLib('ExcelJS');
    el.textContent = 'Gerando…';
    const { wb, nome } = await gerarWorkbook(ctx, scope);
    const buf = await wb.xlsx.writeBuffer();
    await S.dl.save({ filename: nome, data: new Blob([buf], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }) });
    toast('Planilha gerada ✓');
  } catch (e) {
    if (e && e.code === 'declined') toast('Download cancelado');
    else { console.error(e); toast('Não foi possível gerar a planilha: ' + ((e && e.message) || e), 5000); }
  } finally { el.disabled = false; el.textContent = 'Gerar planilha Excel'; }
};
const r3 = (x, d = 3) => (isNum(x) ? Calc.round(x, d) : null);

/* ===== Planilha no formato da contagem impressa ===== */
const VERDE = 'FF1E5B2E', VERDE_CLARO = 'FFE2E8D3', CINZA = 'FFF2F2EE';
const borda = { top: { style: 'thin', color: { argb: 'FF9AA79C' } }, left: { style: 'thin', color: { argb: 'FF9AA79C' } },
  bottom: { style: 'thin', color: { argb: 'FF9AA79C' } }, right: { style: 'thin', color: { argb: 'FF9AA79C' } } };

/* ===== Unificação de várias contagens numa planilha só ===== */
function ctxUnificado(ctxs) {
  ctxs = [...ctxs].sort((x, y) => (x.h.data || '').localeCompare(y.h.data || '') || (x.h.criadoEm || 0) - (y.h.criadoEm || 0));
  const datas = [...new Set(ctxs.map((c) => c.h.data).filter(Boolean))];
  const umaData = datas.length <= 1;
  const conta = {}, nomeAmb = {};
  for (const c of ctxs) for (const a of c.ambs) { conta[a.id] = (conta[a.id] || 0) + 1; nomeAmb[a.id] = nomeAmb[a.id] || a.nome; }
  const ambs = [], mapa = {};
  for (const c of ctxs) for (const a of c.ambs) {
    const id = `${c.h.id}|${a.id}`;
    let nome = a.nome;
    if (!umaData) nome += ` · ${dataBR(c.h.data)}`;
    else if (conta[a.id] > 1) nome += ` · ${c.h.responsavel || c.h.id}`;
    ambs.push({ id, nome, data: c.h.data });
    mapa[id] = [c, a.id];
  }
  const repetidos = Object.keys(conta).filter((k) => conta[k] > 1).map((k) => nomeAmb[k]);
  const resp = [...new Set(ctxs.map((c) => c.h.responsavel).filter(Boolean))].join(', ');
  const h = { id: 'unificada', data: datas[datas.length - 1] || '', responsavel: resp,
    status: ctxs.every((c) => c.h.status === 'finalizada') ? 'finalizada' : 'parcial', obsEnc: '' };
  return { h, ambs, itens: (id) => mapa[id][0].itens(mapa[id][1]), congelado: ctxs.every((c) => c.congelado),
    unif: { datas, n: ctxs.length, repetidos } };
}
const datasUnifTxt = (u) => u.datas.map(dataBR).join(' + ');

const fill = (argb) => ({ type: 'pattern', pattern: 'solid', fgColor: { argb } });
const codCell = (c) => { const n = Number(c); return isFinite(n) && String(n) === String(c) ? n : c; };
const OURO = 'FFB7791F', OURO_CLARO = 'FFFDF1D3', VERMELHO_CLARO = 'FFF8E1DB', VERDE_OK = 'FFDCEBD8';
function cabecalhoVerde(row, cor = VERDE) {
  row.eachCell((c) => {
    c.font = { bold: true, size: 10, color: { argb: 'FFFFFFFF' } };
    c.fill = fill(cor);
    c.alignment = { wrapText: true, vertical: 'middle', horizontal: 'center' };
    c.border = borda;
  });
}
/* valor de um item num ambiente para a planilha: número, 'pendente' ou null (não contado) */
function valorAmb(x) {
  if (!x) return { estado: 'na' };
  const r = x.r;
  if (r.status === 'nao_contado') return { estado: 'nc' };
  if (r.status === 'sem_estoque') return { estado: 'ok', v: 0 };
  if (!r.totalOk || !isNum(r.total)) return { estado: 'pend' };
  return { estado: 'ok', v: r.total };
}

async function gerarWorkbook(ctx, scope) {
  const h = ctx.h;
  const ambs = scope === 'todos' ? ctx.ambs : ctx.ambs.filter((a) => a.id === scope);
  const ids = ambs.map((a) => a.id);
  const rows = montarResumo(ctx, ids);
  const wb = new ExcelJS.Workbook();
  wb.creator = 'Contagem de comida — Mercearia Amauri';
  wb.created = new Date();

  /* ---------- Aba 1: Contagem (layout da planilha CONTAGEM CÂMARAS) ---------- */
  // uma linha por código (variantes do mesmo produto somadas), na ordem alfabética da descrição
  const porCod = new Map();
  for (const row of rows) {
    const k = row.info.codigo;
    if (!porCod.has(k)) porCod.set(k, { info: row.info, linhas: [] });
    porCod.get(k).linhas.push(row);
  }
  const prods = [...porCod.values()].sort((a, b) => collator.compare(a.info.descricao, b.info.descricao));
  const ws = wb.addWorksheet('Contagem', { views: [{ state: 'frozen', xSplit: 3, ySplit: 6 }], pageSetup: { orientation: 'portrait', fitToPage: true, fitToWidth: 1, fitToHeight: 0 } });
  const nA = ambs.length;
  const colTot = 4 + nA, colObs = colTot + 1;
  ws.columns = [{ width: 13 }, { width: 50 }, { width: 7 }, ...ambs.map(() => ({ width: 15 })), { width: 13 }, { width: 46 }];
  const L = (n) => ws.getColumn(n).letter;
  ws.mergeCells(1, 1, 1, colTot);
  const tit = ws.getCell(1, 1);
  tit.value = 'CONTAGEM CÂMARAS';
  tit.font = { bold: true, size: 14 };
  tit.alignment = { horizontal: 'center' };
  const marca = ws.getCell(1, colObs); marca.value = 'RPE'; marca.font = { bold: true, size: 14 }; marca.alignment = { horizontal: 'right' };
  ws.getRow(1).height = 22;
  const rotulo = (linha, texto, valor) => {
    const c = ws.getCell(linha, 1); c.value = texto; c.font = { bold: true, size: 10 };
    const v = ws.getCell(linha, 2); v.value = valor; v.font = { size: 11 };
    v.border = { bottom: { style: 'thin', color: { argb: 'FF000000' } } };
  };
  rotulo(2, ctx.unif ? 'CONTAGENS UNIFICADAS:' : 'DATA DA CONTAGEM:', ctx.unif ? datasUnifTxt(ctx.unif) : `${dataBR(h.data)}${h.tipo && h.tipo !== 'extra' ? ` (${TIPO_CURTO[h.tipo].toLowerCase()})` : ''}`);
  rotulo(3, 'RESPONSÁVEL:', h.responsavel);
  const sit = ws.getCell(2, 4);
  sit.value = ctx.unif ? `Planilha unificada de ${ctx.unif.n} contagens` : `${STATUS_LABEL[h.status] || ''}${h.obsEnc ? ' — ' + h.obsEnc : ''}`;
  sit.font = { size: 10, italic: true, color: { argb: 'FF5A6A5C' } };
  const ger = ws.getCell(3, 4);
  ger.value = `Gerado em ${new Date().toLocaleString('pt-BR')} · ${ctx.congelado ? 'parâmetros do encerramento' : 'cadastro atual'}`;
  ger.font = { size: 10, italic: true, color: { argb: 'FF5A6A5C' } };

  // linha 5: grupo (ex.: COZINHA PRODUÇÃO); linha 6: local (ex.: CÂMARA FRIA)
  const partes = ambs.map((a) => {
    const [g, l] = String(a.nome).split(/\s+[–-]\s+/);
    return l ? { g: ('COZINHA ' + g).toUpperCase().replace('COZINHA COZINHA', 'COZINHA'), l: l.toUpperCase() } : { g: String(a.nome).toUpperCase(), l: 'QUANTIDADE' };
  });
  let i0 = 0;
  while (i0 < nA) {
    let i1 = i0;
    while (i1 + 1 < nA && partes[i1 + 1].g === partes[i0].g) i1++;
    if (i1 > i0) ws.mergeCells(5, 4 + i0, 5, 4 + i1);
    const c = ws.getCell(5, 4 + i0); c.value = partes[i0].g;
    for (let x = i0; x <= i1; x++) { const cc = ws.getCell(5, 4 + x); cc.fill = fill(VERDE); cc.border = borda; cc.font = { bold: true, size: 10, color: { argb: 'FFFFFFFF' } }; cc.alignment = { horizontal: 'center', vertical: 'middle' }; }
    i0 = i1 + 1;
  }
  const hdr = ws.getRow(6);
  ['Item', 'Descrição Item', 'UM', ...partes.map((p) => p.l), 'TOTAL', 'PENDÊNCIAS / OBS.'].forEach((t, i) => { hdr.getCell(i + 1).value = t; });
  cabecalhoVerde(hdr);
  hdr.getCell(colTot).fill = fill(OURO);
  hdr.height = 30;

  let linha = 7;
  for (const pr of prods) {
    const it = pr.info;
    const r = ws.getRow(linha);
    r.getCell(1).value = codCell(it.codigo);
    r.getCell(2).value = it.descricao;
    r.getCell(3).value = it.unidade;
    const fmtN = it.unidade === 'UNID' ? '0' : '#,##0.000';
    const obs = [];
    const ok = [];
    let pend = false, algum = false;
    ambs.forEach((a, j) => {
      const cell = r.getCell(4 + j);
      const vs = pr.linhas.map((row) => valorAmb(row.por[a.id])).filter((v) => v.estado !== 'na');
      if (!vs.length) { cell.fill = fill(CINZA); return; }
      const cont = vs.filter((v) => v.estado !== 'nc');
      if (!cont.length) { cell.value = null; return; }
      algum = true;
      if (cont.some((v) => v.estado === 'pend')) { cell.value = 'pendente'; cell.font = { size: 10, italic: true, color: { argb: 'FF8F5A00' } }; pend = true; return; }
      if (cont.length < vs.length) obs.push(`${a.nome}: variante não contada`);
      cell.value = r3(cont.reduce((s2, v) => s2 + v.v, 0));
      cell.numFmt = fmtN;
      ok.push(4 + j);
    });
    for (const row of pr.linhas) row.pend.forEach((p) => { if (!/não contado$/.test(p)) obs.push(p); });
    for (const row of pr.linhas) row.obs.forEach((o) => obs.push(o));
    const tc = r.getCell(colTot);
    tc.fill = fill(OURO_CLARO); tc.font = { bold: true, size: 11, color: { argb: 'FF6B4A0E' } };
    if (!algum) tc.value = null;
    else if (pend) { tc.value = 'pendente'; tc.font = { size: 10, italic: true, color: { argb: 'FF8F5A00' } }; }
    else {
      const res = ok.reduce((s2, c) => s2 + (Number(r.getCell(c).value) || 0), 0);
      tc.value = { formula: ok.map((c) => `N(${L(c)}${linha})`).join('+') || '0', result: r3(res) };
      tc.numFmt = fmtN;
    }
    r.getCell(colObs).value = [...new Set(obs)].join(' | ') || null;
    for (let x = 1; x <= colObs; x++) {
      const cell = r.getCell(x);
      cell.border = borda;
      if (!cell.font || !cell.font.bold && !cell.font.italic) cell.font = { size: 10.5 };
      cell.alignment = x === 2 || x === colObs ? { horizontal: 'left', wrapText: x === colObs } : { horizontal: 'center' };
      if (linha % 2 === 0 && x > 3 && x < colTot && !cell.fill) cell.fill = fill('FFF7F9F5');
    }
    r.getCell(colObs).font = { size: 9, color: { argb: 'FF5A6A5C' } };
    linha++;
  }
  ws.autoFilter = { from: { row: 6, column: 1 }, to: { row: Math.max(linha - 1, 6), column: colObs } };

  /* ---------- Aba 2: Detalhe por item (fechadas, pesagens e total em cada local) ---------- */
  const wt = wb.addWorksheet('Detalhe por item', { views: [{ state: 'frozen', xSplit: 2, ySplit: 2 }] });
  const th = ['CÓDIGO', 'DESCRIÇÃO', 'VARIANTE', 'CATEGORIA', 'TIPO', 'UM'];
  for (const a of ambs) th.push(`${a.nome} — SITUAÇÃO`, `${a.nome} — FECHADAS / UNID.`, `${a.nome} — PESAGENS`, `${a.nome} — KG PESADOS`, `${a.nome} — TOTAL`);
  th.push('TOTAL GERAL', 'LOCAIS NÃO CONTADOS', 'PENDÊNCIAS', 'OBSERVAÇÕES');
  wt.addRow([ctx.unif ? `Contagens unificadas: ${datasUnifTxt(ctx.unif)} · responsáveis ${h.responsavel}` : `Contagem de ${rotuloC(h)} · responsável ${h.responsavel} · ${STATUS_LABEL[h.status] || ''}`]).font = { bold: true, size: 11 };
  cabecalhoVerde(wt.addRow(th));
  wt.getRow(2).height = 44;
  for (const row of rows) {
    const it = row.info, lt = [codCell(it.codigo), it.descricao, it.varNomeFull || '', it.categoria, TIPO_LABEL[(S.produtos.get(it.codigo) || {}).tipo] || '', it.unidade];
    for (const a of ids) {
      const x = row.por[a];
      if (!x) { lt.push('NÃO SE APLICA', null, null, null, null); continue; }
      const rr = x.r;
      if (rr.status === 'nao_contado') { lt.push('NÃO CONTADO', null, null, null, null); continue; }
      lt.push(sitTxt(rr), x.forma === 'peso' ? null : rr.fechadas, rr.nAbertas, rr.nAbertas ? r3(rr.kgAbertas) : 0, rr.totalOk ? r3(rr.total) : 'pendente');
    }
    const k = row.cons;
    lt.push(k.contados ? r3(k.total) : null, row.naoContadosEm.map((a) => (ambs.find((x) => x.id === a) || {}).nome).join(', '), row.pend.join(' | '), row.obs.join(' | '));
    const rr2 = wt.addRow(lt);
    rr2.eachCell((c) => { c.border = borda; c.font = { size: 10 }; });
  }
  wt.columns.forEach((c, i) => { c.width = i === 1 ? 44 : i === 3 ? 22 : 14; });
  wt.autoFilter = { from: { row: 2, column: 1 }, to: { row: wt.rowCount, column: th.length } };

  /* ---------- Aba 3: Pesagens ---------- */
  const wd = wb.addWorksheet('Pesagens', { views: [{ state: 'frozen', ySplit: 2 }] });
  const dh = ['DATA', 'LOCAL', 'CÓDIGO', 'DESCRIÇÃO', 'VARIANTE', 'Nº', 'PESO NA BALANÇA (kg)', 'TARA DO RECIPIENTE (g)', 'PESO LÍQUIDO (kg)', 'SITUAÇÃO / ALERTAS', 'REGISTRADO POR', 'REGISTRADO EM'];
  wd.addRow([`Pesagens ${ctx.unif ? 'das contagens de ' + datasUnifTxt(ctx.unif) : 'da contagem de ' + rotuloC(h)} — peso líquido = balança − tara do recipiente`]).font = { bold: true, size: 11 };
  cabecalhoVerde(wd.addRow(dh));
  wd.getRow(2).height = 32;
  for (const a of ambs) {
    for (const it of ctx.itens(a.id)) {
      const e = it.e; if (!e || !e.c || e.s || !Array.isArray(e.a) || !e.a.length || it.forma === 'fechadas') continue;
      e.a.forEach((g, ix) => {
        const ar = Calc.aberta(g, it.p, 'KG');
        const ld = wd.addRow([dataBR(a.data || h.data), a.nome, codCell(it.codigo), it.descricao, it.varNomeFull || '', ix + 1,
          r3(g.b / 1000), ar.taraG, null, [ar.erro, ...ar.pendencias, ...ar.alertas].filter(Boolean).join(' | ') || 'OK', e.p || '',
          e.u ? new Date(g.r || e.u).toLocaleString('pt-BR') : '']);
        const n = ld.number;
        if (isNum(ar.kg)) ld.getCell(9).value = { formula: `G${n}-H${n}/1000`, result: r3(ar.kg) };
        ld.getCell(7).numFmt = '#,##0.000'; ld.getCell(9).numFmt = '#,##0.000';
        ld.eachCell((c) => { c.border = borda; c.font = { size: 10 }; });
      });
    }
  }
  if (wd.rowCount === 2) wd.addRow(['Nenhuma pesagem registrada.']);
  wd.columns.forEach((c, i) => { c.width = i === 3 ? 42 : i === 1 ? 26 : i === 9 ? 40 : 14; });
  wd.autoFilter = { from: { row: 2, column: 1 }, to: { row: Math.max(wd.rowCount, 3), column: dh.length } };

  /* ---------- Aba 4: Parâmetros usados ---------- */
  const wp = wb.addWorksheet('Parâmetros usados', { views: [{ state: 'frozen', ySplit: 2 }] });
  const ph = ['CÓDIGO', 'DESCRIÇÃO', 'VARIANTE', 'CATEGORIA', 'UM', 'FORMA DE CONTAGEM', 'EMBALAGEM', 'PESO POR EMBALAGEM / PORÇÃO (g)', 'TARA PADRÃO (g)', 'CAMPOS PENDENTES NO CADASTRO'];
  wp.addRow([ctx.congelado ? 'Parâmetros registrados no encerramento desta contagem' : 'Parâmetros do cadastro atual (contagem não finalizada)']).font = { bold: true, size: 11 };
  cabecalhoVerde(wp.addRow(ph));
  wp.getRow(2).height = 32;
  for (const row of rows) {
    const it = row.info, p = it.p;
    const pend = Calc.camposPendentes({ unidade: it.unidade, forma: it.formaProd || it.forma }, p);
    const lp = wp.addRow([codCell(it.codigo), it.descricao, it.varNomeFull || '', it.categoria, it.unidade,
      FORMA_LABEL[it.formaProd || it.forma], cap(embL(it.embalagem).s), p.pesoLiqG ?? null, p.taraG ?? null, pend.join(', ')]);
    lp.eachCell((c) => { c.border = borda; c.font = { size: 10 }; });
    if (pend.length) lp.getCell(10).font = { size: 10, color: { argb: 'FF8F5A00' } };
  }
  wp.columns.forEach((c, i) => { c.width = i === 1 ? 44 : i === 9 ? 36 : 16; });
  wp.autoFilter = { from: { row: 2, column: 1 }, to: { row: wp.rowCount, column: ph.length } };

  /* ---------- Aba 5: Domingo × Segunda (quando a contagem faz parte de um par) ---------- */
  if (!ctx.unif) {
    let dom = null, seg = null;
    if (h.tipo === 'segunda' && h.par && contagem(h.par)) { const c = await carregarContagem(h.par); dom = ctxDe(contagem(h.par), c.lanc, c.snap); seg = ctx; }
    else if (h.tipo === 'domingo') {
      const hs = segundaDe(h.id);
      if (hs && fechadaStatus(hs.status)) { const c = await carregarContagem(hs.id); dom = ctx; seg = ctxDe(hs, c.lanc, c.snap); }
    }
    if (dom && seg) abaComparacao(wb, dom, seg, scope);
  }

  const sufixo = scope === 'todos' ? '' : ' - ' + ambs[0].nome.replace(/[\\/:*?"<>|–]+/g, ' ').replace(/\s+/g, ' ').trim();
  if (ctx.unif) {
    const d = ctx.unif.datas.map((x) => x.split('-').reverse().join('-'));
    const per = d.length <= 2 ? d.join(' + ') : `${d[0]} a ${d[d.length - 1]}`;
    return { wb, nome: `${per} CONTAGEM COMIDA UNIFICADA.xlsx` };
  }
  const tipoNome = h.tipo === 'domingo' ? ' DOMINGO' : h.tipo === 'segunda' ? ' SEGUNDA' : '';
  return { wb, nome: `${(h.data || '').split('-').reverse().join('-')} CONTAGEM COMIDA${tipoNome}${sufixo}.xlsx` };
}

/* Linhas da comparação domingo × segunda, por local e no total */
function linhasComparacao(A, B, ambIds) {
  const idsA = ambIds.filter((x) => A.ambs.some((a) => a.id === x));
  const idsB = ambIds.filter((x) => B.ambs.some((a) => a.id === x));
  const keys = new Map();
  for (const a of idsA) A.itens(a).forEach((it) => keys.set(it.key, it));
  for (const b of idsB) B.itens(b).forEach((it) => { if (!keys.has(it.key)) keys.set(it.key, it); });
  return [...keys.values()].sort((x, y) => collator.compare(x.categoria, y.categoria) || collator.compare(x.descricao, y.descricao)).map((it) => {
    const va = valorComparado(A, idsA, it.key), vb = valorComparado(B, idsB, it.key);
    const dv = va.estado === 'ok' && vb.estado === 'ok' ? divergencia(va.v, vb.v, it.unidade) : null;
    const porAmb = {};
    for (const a of ambIds) porAmb[a] = { a: valorComparado(A, idsA.includes(a) ? [a] : [], it.key), b: valorComparado(B, idsB.includes(a) ? [a] : [], it.key) };
    return { it, va, vb, dv, porAmb };
  });
}
function abaComparacao(wb, A, B, scope) {
  const ambIds = [...new Set([...A.ambs.map((a) => a.id), ...B.ambs.map((a) => a.id)])].filter((x) => scope === 'todos' || x === scope);
  const nome = (id) => (A.ambs.find((a) => a.id === id) || B.ambs.find((a) => a.id === id) || {}).nome || id;
  const ws = wb.addWorksheet('Domingo x Segunda', { views: [{ state: 'frozen', xSplit: 2, ySplit: 3 }] });
  const c = S.config || {};
  ws.addRow([`Domingo ${dataBR(A.h.data)} (${A.h.responsavel}) × Segunda ${dataBR(B.h.data)} (${B.h.responsavel}) — divergente quando a diferença passa de ${FA(c.limKg ?? 0.5, 3)} kg / ${F(c.limUn ?? 2)} un. ou de ${F(c.limPct ?? 10)}%`]).font = { bold: true, size: 11 };
  ws.addRow([]);
  const th = ['CÓDIGO', 'DESCRIÇÃO', 'VARIANTE', 'UM'];
  for (const a of ambIds) th.push(`${nome(a)} — DOM`, `${nome(a)} — SEG`);
  th.push('TOTAL DOMINGO', 'TOTAL SEGUNDA', 'DIFERENÇA (SEG − DOM)', 'DIFERENÇA %', 'SITUAÇÃO');
  cabecalhoVerde(ws.addRow(th));
  ws.getRow(3).height = 44;
  const lin = linhasComparacao(A, B, ambIds);
  const nCols = th.length;
  const colDom = 5 + ambIds.length * 2, colSeg = colDom + 1, colDif = colDom + 2, colPct = colDom + 3, colSit = colDom + 4;
  const valor = (v) => (v.estado === 'ok' ? r3(v.v) : v.estado === 'nao_contado' || v.estado === 'ausente' ? null : 'pendente');
  for (const l of lin) {
    const it = l.it;
    const vals = [codCell(it.codigo), it.descricao, it.varNomeFull || it.varNome || '', it.unidade];
    for (const a of ambIds) vals.push(valor(l.porAmb[a].a), valor(l.porAmb[a].b));
    vals.push(valor(l.va), valor(l.vb), null, null, null);
    const r = ws.addRow(vals);
    const n = r.number;
    const L = (x) => ws.getColumn(x).letter;
    if (l.dv) {
      r.getCell(colDif).value = { formula: `${L(colSeg)}${n}-${L(colDom)}${n}`, result: r3(l.dv.diff) };
      r.getCell(colPct).value = l.va.v > 0 ? { formula: `${L(colDif)}${n}/${L(colDom)}${n}`, result: r3(l.dv.diff / l.va.v, 4) } : null;
      r.getCell(colPct).numFmt = '0.0%';
      r.getCell(colSit).value = l.dv.div ? 'DIVERGENTE' : 'OK';
    } else r.getCell(colSit).value = l.va.estado !== 'ok' && l.vb.estado !== 'ok' ? 'sem valor nas duas' : l.va.estado !== 'ok' ? 'falta no domingo' : 'falta na segunda';
    const fmtN = it.unidade === 'UNID' ? '0' : '#,##0.000';
    for (let x = 5; x <= colDif; x++) if (typeof r.getCell(x).value === 'number' || (r.getCell(x).value && r.getCell(x).value.formula)) r.getCell(x).numFmt = fmtN;
    r.eachCell({ includeEmpty: true }, (cell, x) => { if (x <= nCols) { cell.border = borda; cell.font = { size: 10 }; } });
    const cor = l.dv ? (l.dv.div ? VERMELHO_CLARO : VERDE_OK) : OURO_CLARO;
    [colDif, colPct, colSit].forEach((x) => { r.getCell(x).fill = fill(cor); });
    if (l.dv && l.dv.div) r.getCell(colSit).font = { size: 10, bold: true, color: { argb: 'FFA8321F' } };
  }
  ws.columns.forEach((col, i) => { col.width = i === 1 ? 44 : i === 2 ? 16 : i === 3 ? 6 : 13; });
  ws.autoFilter = { from: { row: 3, column: 1 }, to: { row: Math.max(ws.rowCount, 4), column: nCols } };
}

/* ===== Histórico e comparação ===== */
VIEWS.hist = () => {
  const u = S.ui;
  const lista = S.contagens.filter((c) => {
    if (u.hDe && c.data < u.hDe) return false;
    if (u.hAte && c.data > u.hAte) return false;
    if (u.hAmb && !(c.ambientes || []).includes(u.hAmb)) return false;
    if (u.hResp && !normBusca(c.responsavel).includes(normBusca(u.hResp))) return false;
    return true;
  }).sort((a, b) => (b.data || '').localeCompare(a.data || '') || (b.criadoEm || 0) - (a.criadoEm || 0));
  const todosAmb = [...new Set(S.contagens.flatMap((c) => c.ambientes || []))];
  const sel = u.cmpSel.filter((id) => contagem(id));
  const modo = !!u.junModo;
  const jsel = (u.junSel || []).filter((id) => contagem(id));
  if (modo) return histJuntar(lista, jsel);
  return `${topbar({ back: '#/gestao', eyebrow: 'Contagens anteriores', title: 'Histórico' })}
  <div class="stack" style="margin-top:14px">
    <div class="grid2">
      <label class="f">De<input type="date" id="h-de" data-ch="hf" data-k="hDe" value="${esc(u.hDe)}"></label>
      <label class="f">Até<input type="date" id="h-ate" data-ch="hf" data-k="hAte" value="${esc(u.hAte)}"></label>
      <label class="f">Ambiente<select id="h-amb" data-ch="hf" data-k="hAmb"><option value="">Todos</option>${todosAmb.map((a) => `<option value="${a}" ${u.hAmb === a ? 'selected' : ''}>${esc(ambNome(a))}</option>`).join('')}</select></label>
      <label class="f">Responsável<input id="h-resp" data-in="hResp" value="${esc(u.hResp)}" placeholder="Nome"></label>
    </div>
    <button class="btn big pri full" data-act="junModo">Juntar contagens numa planilha</button>
    <div class="note n-info small">A comparação Domingo × Segunda fica no botão de cada contagem. Para comparar outras duas, marque duas encerradas. ${sel.length ? `<b>${sel.length} selecionada(s).</b>` : ''}</div>
    ${sel.length === 2 ? `<button class="btn big pri full" data-act="comparar">Comparar as 2 contagens</button>` : ''}
    <div id="hlist" class="stack">${lista.length ? lista.map((c) => {
      const fech = fechadaStatus(c.status);
      const pill = c.status === 'finalizada' ? 'p-ok' : c.status === 'parcial' ? 'p-warn' : c.status === 'pausada' ? 'p-pend' : 'p-acc';
      return `<div class="card stack" style="gap:8px">
        <div class="row between"><h3>${rotuloC(c)}</h3><span class="pill ${pill}">${STATUS_LABEL[c.status]}</span></div>
        <div class="small muted">Responsável: ${esc(c.responsavel)} · ${(c.ambientes || []).map((a) => esc(ambNome(a, c))).join(', ')}</div>
        ${c.resumoEnc && (c.resumoEnc.naoContados || c.resumoEnc.calcPendentes) ? `<div class="small" style="color:var(--amber)">${c.resumoEnc.naoContados} não contados · ${c.resumoEnc.calcPendentes} cálculos pendentes</div>` : ''}
        <div class="row">
          <button class="btn grow" data-act="go" data-to="${fech ? `#/c/${c.id}/resumo` : `#/c/${c.id}`}">${fech ? 'Resumo e Excel' : 'Continuar'}</button>
          ${(() => { const par = fech && c.tipo === 'segunda' && c.par && contagem(c.par) ? [c.par, c.id] : fech && c.tipo === 'domingo' && segundaDe(c.id) && fechadaStatus(segundaDe(c.id).status) ? [c.id, segundaDe(c.id).id] : null;
            return par ? `<button class="btn pri grow" data-act="go" data-to="#/comparar/${par[0]}/${par[1]}">Domingo × Segunda</button>` : ''; })()}
          ${fech ? `<label class="check" style="flex:none"><input type="checkbox" data-ch="cmpSel" value="${c.id}" ${sel.includes(c.id) ? 'checked' : ''}> Comparar</label>` : ''}
        </div></div>`;
    }).join('') : '<p class="muted">Nenhuma contagem encontrada com esses filtros.</p>'}</div>
  </div>`;
};
INP.hf = (el) => { S.ui[el.dataset.k] = el.value; render(); };
INP.hResp = (el) => { S.ui.hResp = el.value; render(); };
INP.cmpSel = (el) => {
  let s = S.ui.cmpSel.filter((x) => x !== el.value);
  if (el.checked) s.push(el.value);
  if (s.length > 2) s = s.slice(-2);
  S.ui.cmpSel = s; render();
};
function histJuntar(lista, jsel) {
  const fech = lista.filter((c) => fechadaStatus(c.status));
  const dias = [...new Set(fech.map((c) => c.data))].slice(0, 8);
  const conta = {};
  jsel.map(contagem).forEach((c) => (c.ambientes || []).forEach((a) => { conta[a] = (conta[a] || 0) + 1; }));
  const rep = Object.keys(conta).filter((a) => conta[a] > 1).map((a) => esc(ambNome(a)));
  return `${topbar({ back: '#/hist', eyebrow: 'Histórico', title: 'Juntar contagens' })}
  <div class="stack" style="margin-top:14px">
    <div class="note n-info small"><b>Toque nas contagens que quer juntar</b> numa planilha só. Cada ambiente de cada contagem vira um bloco, com o total geral no final.${driveAtivo() ? ' A planilha vai direto para a pasta do Drive e também baixa no aparelho.' : ''}</div>
    ${dias.length ? `<div class="small muted">Marcar todas de um dia:</div>
    <div class="chips">${dias.map((d) => `<button class="chip" data-act="junDia" data-d="${esc(d)}">${dataBR(d).slice(0, 5)}</button>`).join('')}</div>` : ''}
    ${S.ui.junRes ? (S.ui.junRes.ok
      ? `<div class="note n-ok small"><b>Enviada para a pasta do Drive:</b> ${esc(S.ui.junRes.nome)}<br><a href="${esc(S.ui.junRes.link)}" target="_blank" rel="noopener">Abrir no Drive</a></div>`
      : `<div class="note n-err small"><b>Não foi para o Drive:</b> ${esc(S.ui.junRes.msg || '')}. A planilha foi baixada no aparelho; tente juntar de novo mais tarde.</div>`) : ''}
    ${rep.length ? `<div class="note n-warn small"><b>${rep.join(', ')}</b> aparece em mais de uma contagem marcada. Cada uma sai num bloco separado, mas o total geral soma as duas.</div>` : ''}
    <div class="stack">${fech.length ? fech.map((c) => {
      const on = jsel.includes(c.id);
      const pill = c.status === 'finalizada' ? 'p-ok' : 'p-warn';
      return `<button class="card tap jun ${on ? 'on' : ''}" data-act="junTog" data-id="${c.id}" aria-pressed="${on}">
        <div class="row" style="flex-wrap:nowrap;align-items:flex-start">
          <span class="jbox">${on ? '✓' : ''}</span>
          <div class="grow stack" style="gap:4px">
            <div class="row between"><h3>${rotuloC(c)}</h3><span class="pill ${pill}">${STATUS_LABEL[c.status]}</span></div>
            <div class="small muted">${esc(c.responsavel)} · ${(c.ambientes || []).map((a) => esc(ambNome(a, c))).join(', ')}</div>
            ${c.resumoEnc && (c.resumoEnc.naoContados || c.resumoEnc.calcPendentes) ? `<div class="small" style="color:var(--amber)">${c.resumoEnc.naoContados} não contados · ${c.resumoEnc.calcPendentes} cálculos pendentes</div>` : ''}
          </div>
        </div></button>`;
    }).join('') : '<p class="muted">Nenhuma contagem encerrada para juntar.</p>'}</div>
    <p class="small muted" style="margin:0">Só aparecem contagens encerradas.</p>
  </div>
  <div class="bar"><div class="in">
    <button class="btn big" data-act="junSair" style="flex:0 0 auto">Cancelar</button>
    <button class="btn big pri" data-act="unificar" ${jsel.length < 2 || !S.dl ? 'disabled' : ''}>${jsel.length < 2 ? 'Marque 2 ou mais' : `Juntar ${jsel.length} contagens`}</button>
  </div></div>`;
}
ACT.junModo = () => { S.ui.junRes = null; S.ui.junModo = true; S.ui.junSel = S.ui.junSel || []; render(); window.scrollTo(0, 0); };
ACT.junSair = () => { S.ui.junModo = false; render(); };
ACT.junTog = (el) => {
  const id = el.dataset.id; let s = (S.ui.junSel || []).filter((x) => x !== id);
  if (!(S.ui.junSel || []).includes(id)) s.push(id);
  S.ui.junSel = s; render();
};
ACT.junDia = (el) => {
  const ids = S.contagens.filter((c) => c.data === el.dataset.d && fechadaStatus(c.status)).map((c) => c.id);
  const s = new Set(S.ui.junSel || []);
  const todas = ids.every((id) => s.has(id));
  ids.forEach((id) => (todas ? s.delete(id) : s.add(id)));
  S.ui.junSel = [...s]; render();
};
ACT.unificar = async (el) => {
  if (!S.dl) return toast('Download indisponível nesta visualização.', 4000);
  const ids = (S.ui.junSel || []).filter((id) => contagem(id));
  if (ids.length < 2) return toast('Marque pelo menos 2 contagens');
  el.disabled = true; el.textContent = 'Carregando…';
  try {
    const ctxs = [];
    for (const id of ids) { const h = contagem(id); const c = await carregarContagem(id); ctxs.push(ctxDe(h, c.lanc, c.snap)); }
    await carregarLib('ExcelJS');
    el.textContent = 'Gerando planilha…';
    const { wb, nome } = await gerarWorkbook(ctxUnificado(ctxs), 'todos');
    const buf = await wb.xlsx.writeBuffer();
    let drive = null;
    if (driveAtivo()) {
      el.textContent = 'Enviando para o Drive…';
      drive = await subirNoDrive(nome, new Uint8Array(buf));
      S.ui.junRes = drive.ok ? { ok: true, nome, link: drive.link } : { ok: false, nome, msg: drive.msg };
      render();
    }
    try {
      await S.dl.save({ filename: nome, data: new Blob([buf], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' }) });
    } catch (e2) { if (!(e2 && e2.code === 'declined')) console.error(e2); }
    toast(!drive ? 'Planilha unificada gerada ✓' : drive.ok ? 'Planilha enviada para o Drive e baixada ✓' : 'Planilha baixada, mas não foi para o Drive', 4000);
  } catch (e) {
    if (e && e.code === 'declined') toast('Download cancelado');
    else { console.error(e); toast('Não foi possível gerar a planilha: ' + ((e && e.message) || e), 5000); }
  } finally { el.disabled = false; el.textContent = `Juntar ${ids.length} contagens`; }
};
ACT.comparar = () => {
  const [a, b] = S.ui.cmpSel.map(contagem).sort((x, y) => (x.data || '').localeCompare(y.data || '') || (x.criadoEm || 0) - (y.criadoEm || 0));
  go(`#/comparar/${a.id}/${b.id}`);
};
async function carregarContagem(cid) {
  if (S.cache[cid]) return S.cache[cid];
  const h = contagem(cid);
  const lanc = {};
  for (const a of h.ambientes || []) { const d = await S.db.doc(`contagens/${cid}/lanc/${a}`).get(); lanc[a] = d.exists ? d.data().i || {} : {}; }
  const sn = await S.db.doc(`contagens/${cid}/snap/params`).get();
  S.cache[cid] = { lanc, snap: sn.exists ? sn.data() : null };
  return S.cache[cid];
}
function valorComparado(ctx, ambIds, key) {
  const presentes = ambIds.filter((a) => ctx.itens(a).some((x) => x.key === key));
  if (!presentes.length) return { estado: 'ausente' };
  const its = presentes.map((a) => ctx.itens(a).find((x) => x.key === key));
  const k = Calc.consolidar(its.map((x) => x.r), its[0].unidade);
  if (!k.contados) return { estado: 'nao_contado', it: its[0] };
  if (!isNum(k.total)) return { estado: 'pendente', it: its[0] };
  return { estado: 'ok', v: k.total, it: its[0] };
}
VIEWS.comparar = (r) => {
  const ha = contagem(r.a), hb = contagem(r.b);
  if (!ha || !hb) return `${topbar({ back: '#/hist', title: 'Comparação' })}<p class="muted">Contagem não encontrada.</p>`;
  const ca = S.cache[r.a], cb = S.cache[r.b];
  if (!ca || !cb) {
    Promise.all([carregarContagem(r.a), carregarContagem(r.b)]).then(() => { if (R.name === 'comparar') render(); }).catch((e) => toast(errMsg(e), 4000));
    return `${topbar({ back: '#/hist', title: 'Carregando comparação…' })}`;
  }
  const A = ctxDe(ha, ca.lanc, ca.snap), B = ctxDe(hb, cb.lanc, cb.snap);
  const par = hb.par === ha.id;
  const union = [...new Set([...(ha.ambientes || []), ...(hb.ambientes || [])])];
  const esc2 = S.ui.cmpAmb === 'cons' || union.includes(S.ui.cmpAmb) ? S.ui.cmpAmb : 'cons';
  const faltaA = union.filter((x) => !ha.ambientes.includes(x)), faltaB = union.filter((x) => !hb.ambientes.includes(x));
  const lin = linhasComparacao(A, B, esc2 === 'cons' ? union : [esc2]);
  const filtro = S.ui.cmpFiltro || (par ? 'div' : 'todos');
  const nDiv = lin.filter((l) => l.dv && l.dv.div).length;
  const nFalt = lin.filter((l) => !l.dv).length;
  let body = '', cat = null, n = 0;
  const cel = (v) => v.estado === 'ok' ? qtdTxt(v.v, v.it.unidade)
    : v.estado === 'ausente' ? '<span class="pill p-err">faltante</span>' : v.estado === 'nao_contado' ? '<span class="pill p-pend">não contado</span>' : '<span class="pill p-warn">pendente</span>';
  for (const l of lin) {
    const it = l.it, dv = l.dv;
    if (filtro === 'div' && !(dv && dv.div)) continue;
    if (filtro === 'var' && !(dv && Math.abs(dv.diff) > 1e-9)) continue;
    if (filtro === 'falt' && dv) continue;
    if (it.categoria !== cat) { cat = it.categoria; body += `<tr class="cat"><td colspan="4">${esc(cat)}</td></tr>`; }
    n++;
    const dtxt = dv ? `<b style="color:${dv.div ? 'var(--danger)' : 'var(--ok)'}">${dv.diff > 0 ? '+' : ''}${it.unidade === 'UNID' ? F(dv.diff) : FA(dv.diff, 3)}${l.va.v > 0 && Math.abs(dv.diff) > 1e-9 ? `<br><span class="tiny">${dv.diff > 0 ? '+' : ''}${F(dv.diff / l.va.v * 100)}%</span>` : ''}${dv.div ? ' ⚠' : ''}</b>` : '<span class="muted">—</span>';
    body += `<tr ${dv && dv.div ? 'style="background:var(--danger-soft)"' : ''}><td><b>${esc(it.descricao)}</b>${it.varNome ? ` · ${esc(it.varNome)}` : ''}<br><span class="tiny muted">${esc(it.codigo)} · ${it.unidade}</span></td>
      <td class="n">${cel(l.va)}</td><td class="n">${cel(l.vb)}</td><td class="n">${dtxt}</td></tr>`;
  }
  const c = S.config || {};
  return `${topbar({ back: '#/hist', eyebrow: par ? 'Domingo × Segunda' : 'Variação de estoque contado', title: `${esc(rotuloC(ha))} → ${esc(rotuloC(hb))}` })}
  <div class="stack" style="margin-top:14px">
    <div class="grid2">
      <div class="card"><div class="eyebrow">Divergentes</div><div class="num" style="font-size:28px;font-weight:700;font-family:var(--f-head);color:${nDiv ? 'var(--danger)' : 'var(--ok)'}">${nDiv}</div></div>
      <div class="card"><div class="eyebrow">Sem contagem em uma delas</div><div class="num" style="font-size:28px;font-weight:700;font-family:var(--f-head)">${nFalt}</div></div>
    </div>
    <div class="note n-info small">Diferença entre o que foi <b>contado</b> (B − A). ⚠ = passou de <b>${FA(c.limKg ?? 0.5, 3)} kg</b> (ou ${F(c.limUn ?? 2)} un.) <b>ou</b> de <b>${F(c.limPct ?? 10)}%</b>${ehGestor() ? ' — ajuste em Cadastro › Ajustes' : ''}. ${par ? 'Entre domingo e segunda a cozinha pode ter produzido ou usado itens: confira antes de concluir que houve erro.' : ''}</div>
    ${faltaA.length ? `<div class="note n-warn small">Local fora da contagem A: <b>${faltaA.map((x) => esc(ambNome(x))).join(', ')}</b></div>` : ''}
    ${faltaB.length ? `<div class="note n-warn small">Local fora da contagem B: <b>${faltaB.map((x) => esc(ambNome(x))).join(', ')}</b></div>` : ''}
    <div class="chips"><button class="chip ${esc2 === 'cons' ? 'on' : ''}" data-act="cmpAmb" data-k="cons">Todos os locais</button>${union.map((a) => `<button class="chip ${esc2 === a ? 'on' : ''}" data-act="cmpAmb" data-k="${a}">${esc(ambNome(a))}</button>`).join('')}</div>
    <div class="seg">${[['div', `Divergentes (${nDiv})`], ['var', 'Com diferença'], ['falt', 'Faltantes'], ['todos', 'Todos']].map(([k, l]) => `<button data-act="cmpFiltro" data-k="${k}" class="${filtro === k ? 'on' : ''}">${l}</button>`).join('')}</div>
    <div class="small muted">${n} itens</div>
    <div class="tw"><table><thead><tr><th>Produto</th><th class="n">A · ${esc(rotuloC(ha))}</th><th class="n">B · ${esc(rotuloC(hb))}</th><th class="n">Diferença</th></tr></thead><tbody>${body || '<tr><td colspan="4" class="muted">Nada neste filtro.</td></tr>'}</tbody></table></div>
    ${par ? `<button class="btn full" data-act="go" data-to="#/c/${hb.id}/resumo">Resumo e Excel da segunda (inclui a aba Domingo × Segunda)</button>` : ''}
  </div>`;
};
ACT.cmpAmb = (el) => { S.ui.cmpAmb = el.dataset.k; render(); };
ACT.cmpFiltro = (el) => { S.ui.cmpFiltro = el.dataset.k; render(); };

/* ===== Cadastro ===== */
function pendCount(p) {
  return (p.variantes || []).filter((v) => v.ativo !== false).reduce((n, v) => n + Calc.camposPendentes(p, v).length, 0);
}
function cadTabs(tab) {
  return `<div class="seg" style="margin-top:12px">${[['prod', 'Produtos'], ['amb', 'Ambientes'], ['ajustes', 'Taras'], ['equipe', 'Equipe'], ['imp', 'Importar']].map(([k, l]) =>
    `<button data-act="go" data-to="#/cad/${k}" class="${tab === k ? 'on' : ''}">${l}</button>`).join('')}</div>`;
}
VIEWS.cad = (r) => {
  const tab = r.tab;
  const head = `${topbar({ back: '#/gestao', eyebrow: 'Cadastro', title: tab === 'amb' ? 'Ambientes (câmaras)' : tab === 'equipe' ? 'Equipe' : tab === 'imp' ? 'Importar planilha' : tab === 'ajustes' ? 'Recipientes e limites' : 'Produtos' })}${cadTabs(tab)}`;
  if (tab === 'equipe') return head + viewEquipe();
  if (tab === 'amb') return head + viewAmbientes();
  if (tab === 'imp') return head + viewImportar();
  if (tab === 'ajustes') return head + viewAjustes();
  const u = S.ui;
  const cats = categorias();
  const q = normBusca(u.cadBusca.trim());
  const todos = produtosOrdenados();
  const comPend = todos.filter((p) => p.ativo !== false && pendCount(p)).length;
  const comConf = todos.filter((p) => p.ativo !== false && (p.conferir || []).length).length;
  const lista = todos.filter((p) => {
    if (u.cadFiltro === 'pend' && !(p.ativo !== false && pendCount(p))) return false;
    if (u.cadFiltro === 'conf' && !(p.ativo !== false && (p.conferir || []).length)) return false;
    if (u.cadFiltro === 'inat' && p.ativo !== false) return false;
    if (u.cadFiltro === 'todos' && p.ativo === false) return false;
    if (u.cadCat && p.categoria !== u.cadCat) return false;
    if (q && !normBusca(`${p.codigo} ${p.descricao} ${p.marca || ''}`).includes(q)) return false;
    return true;
  });
  return `${head}<div class="stack" style="margin-top:14px">
    <button class="btn big pri full" data-act="go" data-to="#/cad/p/novo">Novo produto</button>
    <div class="search">${ICON.search}<input id="cb" type="search" data-in="cadBusca" placeholder="Buscar por nome ou código" value="${esc(u.cadBusca)}"></div>
    <select id="ccat" data-ch="cadCat"><option value="">Todas as categorias</option>${cats.map((c) => `<option ${u.cadCat === c ? 'selected' : ''}>${esc(c)}</option>`).join('')}</select>
    <div class="seg">${[['todos', 'Ativos'], ['conf', `A conferir (${comConf})`], ['pend', `Sem peso da porção (${comPend})`], ['inat', 'Inativos']].map(([k, l]) => `<button data-act="cadFiltro" data-k="${k}" class="${u.cadFiltro === k ? 'on' : ''}">${l}</button>`).join('')}</div>
    ${comConf ? `<div class="note n-warn small"><b>${comConf} produtos a conferir</b>: vieram da planilha sem código, sem unidade ou com unidade suspeita. Abra cada um, confira e toque em “Marcar como conferido”.</div>` : ''}
    ${comPend ? '<div class="note n-warn small">Alguns produtos contam embalagens/porções fechadas mas não têm o peso de cada uma. Preencha para os kg saírem completos.</div>' : ''}
    <div class="small muted">${lista.length} produtos</div>
    <div id="clist">${cadListaHTML(lista)}</div>
  </div>`;
};
function cadListaHTML(lista) {
  let out = '', cat = null, open = false;
  for (const p of lista.slice(0, 400)) {
    if (p.categoria !== cat) { if (open) out += '</div>'; cat = p.categoria; out += `<div class="eyebrow" style="margin:14px 0 6px">${esc(cat)}</div><div class="list">`; open = true; }
    const n = pendCount(p);
    out += `<div class="li">
      <button class="qthumb" data-act="fotoItem" data-key="${esc(p.codigo)}" aria-label="Foto de ${esc(p.descricao)}">${miniHTML(p)}</button>
      <button class="grow" style="background:none;border:0;padding:0;text-align:left;font:inherit;color:inherit" data-act="go" data-to="#/cad/p/${encodeURIComponent(p.codigo)}"><span class="nm">${esc(p.descricao)}</span><br><span class="meta">${esc(p.codigo)} · ${p.unidade} · ${p.tipo ? esc(p.tipo === 'INSUMO' ? 'insumo' : p.tipo) + ' · ' : ''}${FORMA_LABEL[p.forma]}${(p.variantes || []).length > 1 ? ` · ${(p.variantes || []).length} variantes` : ''}</span></button>
      ${p.ativo === false ? '<span class="pill p-pend">Inativo</span>' : (p.conferir || []).length ? '<span class="pill p-warn">Conferir</span>' : n ? `<span class="pill p-warn">${n} pendente${n > 1 ? 's' : ''}</span>` : '<span class="pill p-ok">Completo</span>'}</div>`;
  }
  return (out + (open ? '</div>' : '')) || '<p class="muted">Nenhum produto.</p>';
}
INP.cadBusca = (el) => { S.ui.cadBusca = el.value; render(); };
INP.cadCat = (el) => { S.ui.cadCat = el.value; render(); };
ACT.cadFiltro = (el) => { S.ui.cadFiltro = el.dataset.k; render(); };

/* ---- Editor de produto ---- */
const NUMF = [
  ['pesoLiqG', 'Peso de cada embalagem / porção fechada', 'g', 'Ex.: porção de 160 g, pacote de 2.000 g. Usado para transformar as fechadas em kg. Deixe vazio se o item é só pesado.'],
  ['taraG', 'Tara padrão (recipiente)', 'g', 'Peso do recipiente em que o item costuma ficar (GN, pote). 0 = pesado direto (vácuo, pacote). Vazio = escolher o recipiente a cada pesagem.'],
];
function novoProdutoVazio() {
  const amb = {}; ambAtivos().forEach((a) => { amb[a.id] = { abertas: true }; });
  return { codigo: '', descricao: '', categoria: '', marca: '', tipo: 'INSUMO', marcas: [], conferir: [], unidade: 'KG', forma: 'peso', amb, foto: null, obs: '', ativo: true,
    variantes: [{ id: 'v1', nome: 'Padrão', embalagem: 'pacote', volumeMl: null, pesoLiqG: null, taraG: 0, pesoCheioG: null, densidade: null, metodo: '', condPesagem: '', ativo: true, notas: {} }] };
}
function pdraftFor(code) {
  if (S.ui.pdraft && S.ui.pdraft._code === code) return S.ui.pdraft;
  const base = code === 'novo' ? novoProdutoVazio() : S.produtos.get(code);
  if (!base) return null;
  const d = JSON.parse(JSON.stringify(base));
  d._code = code; d._novo = code === 'novo';
  d.variantes.forEach((v) => { NUMF.forEach(([k]) => { v['_' + k] = numIn(v[k]); }); });
  S.ui.pdraft = d; return d;
}
VIEWS.produto = (r) => {
  const d = pdraftFor(r.code);
  if (!d) return `${topbar({ back: '#/cad', title: 'Produto não encontrado' })}`;
  const cats = categorias();
  const pendTotal = d.variantes.filter((v) => v.ativo !== false).reduce((n, v) => n + Calc.camposPendentes(d, v).length, 0);
  return `${topbar({ back: '#/cad', eyebrow: d._novo ? 'Novo produto' : 'Editar produto', title: esc(d.descricao || 'Sem descrição') })}
  <div class="stack" style="margin-top:14px">
    ${pendTotal ? `<div class="note n-warn small">${pendTotal} campo(s) destacado(s) precisam ser preenchidos para os cálculos saírem completos.</div>` : ''}
    <section class="card stack">
      <h3>Identificação</h3>
      <label class="f">Código${d._novo ? '' : ' <span class="hint">(não editável — identifica o produto no histórico)</span>'}<input id="p-codigo" data-in="pf" data-k="codigo" value="${esc(d.codigo)}" ${d._novo ? '' : 'readonly'} inputmode="numeric"></label>
      <label class="f">Nome / descrição<input id="p-desc" data-in="pf" data-k="descricao" value="${esc(d.descricao)}"></label>
      <label class="f">Categoria<input id="p-cat" data-in="pf" data-k="categoria" value="${esc(d.categoria)}" list="dl-cats"><datalist id="dl-cats">${cats.map((c) => `<option value="${esc(c)}">`).join('')}</datalist></label>
      <label class="f">Marca <span class="hint">opcional</span><input id="p-marca" data-in="pf" data-k="marca" value="${esc(d.marca || '')}"></label>
      <div class="grid2">
        <label class="f">Unidade<select id="p-un" data-ch="pfs" data-k="unidade">${['KG', 'UNID'].map((u) => `<option ${d.unidade === u ? 'selected' : ''}>${u}</option>`).join('')}</select></label>
        <label class="f">Forma de contagem<select id="p-forma" data-ch="pfs" data-k="forma">${Object.entries(FORMA_LABEL).map(([k, l]) => `<option value="${k}" ${d.forma === k ? 'selected' : ''}>${l}</option>`).join('')}</select></label>
      </div>
      <label class="f">Tipo<select id="p-tipo" data-ch="pfs" data-k="tipo">${Object.entries(TIPO_LABEL).map(([k, l]) => `<option value="${k}" ${(d.tipo || 'INSUMO') === k ? 'selected' : ''}>${l}</option>`).join('')}</select></label>
      <div class="small muted">Marcas que usam: ${Object.entries(Defaults.MARCAS).map(([k, l]) => `<label class="check small" style="display:inline-flex;margin-right:10px"><input type="checkbox" data-ch="pmarca" value="${k}" ${(d.marcas || []).includes(k) ? 'checked' : ''}> ${k}</label>`).join('')}</div>
      <label class="check"><input type="checkbox" id="p-ativo" data-ch="pfc" data-k="ativo" ${d.ativo !== false ? 'checked' : ''}> Produto ativo</label>
      ${(d.conferir || []).length ? `<div class="note n-warn small"><b>Conferir:</b> ${d.conferir.map(esc).join(' · ')}<br><button class="btn small" style="margin-top:8px;min-height:38px" data-act="pconferido">Marcar como conferido</button></div>` : ''}
    </section>

    <section class="card stack">
      <div class="row between"><h3>Embalagens / variantes</h3><button class="btn" data-act="addVar">+ Variante</button></div>
      <p class="small muted" style="margin:0">Cada tamanho é uma variante contada separado. Ex.: Ancho porção 400 g e porção 600 g. Na planilha final as variantes do mesmo código são somadas.</p>
      ${d.variantes.map((v, ix) => varHTML(d, v, ix)).join('')}
    </section>

    <section class="card stack">
      <h3>Ambientes em que é contado</h3>
      ${ambAtivos().map((a) => { const c = d.amb && d.amb[a.id]; return `<div class="row between" style="border-bottom:1px solid var(--line);padding-bottom:8px">
        <label class="check"><input type="checkbox" id="pa-${a.id}" data-ch="pamb" data-a="${a.id}" ${c ? 'checked' : ''}> ${esc(a.nome)}</label>
        ${d.forma === 'fechadas_abertas' ? `<label class="check small"><input type="checkbox" id="pab-${a.id}" data-ch="pambab" data-a="${a.id}" ${c && c.abertas !== false ? 'checked' : ''} ${c ? '' : 'disabled'}> pesar abertas</label>` : ''}</div>`; }).join('')}
    </section>

    <section class="card stack">
      <h3>Foto e observações</h3>
      ${fotoSrc(d) ? `<img src="${esc(fotoSrc(d))}" alt="" style="max-width:160px;border-radius:12px">` : ''}
      ${d._novo ? '<p class="small muted" style="margin:0">Salve o produto primeiro; depois toque na miniatura dele na lista para pôr a foto.</p>'
        : `<button class="btn ghost full" data-act="fotoItem" data-key="${esc(d.codigo)}">${fotoSrc(d) ? 'Trocar a foto' : 'Tirar foto ou escolher do celular'}</button>`}
      <label class="f">Observações<textarea id="p-obs" data-in="pf" data-k="obs">${esc(d.obs || '')}</textarea></label>
      ${d.origem ? `<div class="tiny muted">Origem: ${esc(d.origem)}</div>` : ''}
    </section>
  </div>
  <div class="bar"><div class="in"><button class="btn big" data-act="cancelProd">Descartar</button><button class="btn big pri" data-act="salvarProd" id="b-sp">Salvar produto</button></div></div>`;
};
/* Avisa quando o volume nominal parece ter a tara somada (1 g de bebida = 1 ml) */
function avisoVolume() { return ''; }
function varHTML(d, v, ix) {
  const pend = new Set(Calc.camposPendentes(d, v));
  const need = (k) => ({ pesoLiqG: 'peso por embalagem/porção' }[k]);
  const conv = Calc.conversao({ ...v, ...Object.fromEntries(NUMF.map(([k]) => [k, parseF(v['_' + k]).value])) });
  return `<div class="garrafa stack" style="gap:10px">
    <div class="row between"><b>Variante ${ix + 1}</b>${d.variantes.length > 1 ? `<button class="btn ghost small" data-act="rmVar" data-ix="${ix}">Remover</button>` : ''}</div>
    <div class="grid2">
      <label class="f">Nome da variante<input id="v${ix}-nome" data-in="vf" data-ix="${ix}" data-k="nome" value="${esc(v.nome)}" placeholder="ex.: Porção 160 g"></label>
      <label class="f">Tipo de embalagem<select id="v${ix}-emb" data-ch="vfs" data-ix="${ix}" data-k="embalagem">${Object.keys(EMB).map((k) => `<option value="${k}" ${v.embalagem === k ? 'selected' : ''}>${cap(EMB[k].s)}</option>`).join('')}</select></label>
    </div>
    <div class="stack" style="gap:10px">${NUMF.map(([k, l, u, hint]) => `<label class="f">${l}<div class="unitwrap"><input id="v${ix}-${k}" inputmode="decimal" data-in="vn" data-ix="${ix}" data-k="${k}" value="${esc(v['_' + k] || '')}" class="${pend.has(need(k)) ? 'need' : ''}"><span class="u">${u}</span></div>
      <span class="hint">${v.notas && v.notas[k] ? `<b>${esc(v.notas[k])}.</b> ` : ''}${hint}</span></label>`).join('')}</div>
    <label class="check"><input type="checkbox" data-ch="vfc" data-ix="${ix}" data-k="ativo" ${v.ativo !== false ? 'checked' : ''}> Variante ativa</label>
    ${avisoVolume(d, v)}
    ${pend.size ? `<div class="note n-warn small">Falta: ${[...pend].join(', ')}</div>` : `<div class="note n-ok small">Parâmetros completos.</div>`}
  </div>`;
}
const parseF = (s) => Calc.parseNum(s, { thousandsDot: true });
const parseD = (s) => Calc.parseNum(s, { thousandsDot: false });
INP.pf = (el) => { S.ui.pdraft[el.dataset.k] = el.value; };
INP.pfs = (el) => { S.ui.pdraft[el.dataset.k] = el.value; render(); };
INP.pfc = (el) => { S.ui.pdraft[el.dataset.k] = el.checked; };
INP.pamb = (el) => {
  const d = S.ui.pdraft; d.amb = d.amb || {};
  if (el.checked) d.amb[el.dataset.a] = { abertas: true };
  else delete d.amb[el.dataset.a];
  render();
};
INP.pmarca = (el) => {
  const d = S.ui.pdraft; const m = new Set(d.marcas || []);
  el.checked ? m.add(el.value) : m.delete(el.value); d.marcas = [...m];
};
ACT.pconferido = () => { S.ui.pdraft.conferir = []; render(); toast('Ok — salve o produto para gravar'); };
INP.pambab = (el) => { const d = S.ui.pdraft; if (d.amb[el.dataset.a]) d.amb[el.dataset.a].abertas = el.checked; };
INP.vf = (el) => { S.ui.pdraft.variantes[+el.dataset.ix][el.dataset.k] = el.value; };
INP.vfs = (el) => { S.ui.pdraft.variantes[+el.dataset.ix][el.dataset.k] = el.value; render(); };
INP.vfc = (el) => { S.ui.pdraft.variantes[+el.dataset.ix][el.dataset.k] = el.checked; render(); };
INP.vn = (el) => {
  const v = S.ui.pdraft.variantes[+el.dataset.ix];
  v['_' + el.dataset.k] = el.value;
  if (v.notas) delete v.notas[el.dataset.k];
  const pr = (el.dataset.k === 'densidade' ? parseD : parseF)(el.value);
  el.classList.toggle('bad', !pr.ok);
};
ACT.addVar = () => {
  const d = S.ui.pdraft;
  const base = d.variantes[0];
  d.variantes.push({ id: uid('v'), nome: '', embalagem: base ? base.embalagem : 'pote', volumeMl: null, pesoLiqG: null, taraG: null, pesoCheioG: null, densidade: null, metodo: '', condPesagem: '', ativo: true, notas: {},
    ...Object.fromEntries(NUMF.map(([k]) => ['_' + k, ''])) });
  render();
};
ACT.rmVar = async (el) => {
  const d = S.ui.pdraft; const ix = +el.dataset.ix;
  if (!(await confirmar('Remover variante?', 'Contagens já feitas com ela continuam no histórico. Para apenas parar de contar, desmarque “Variante ativa”.', 'Remover', 'dan'))) return;
  d.variantes.splice(ix, 1); render();
};
INP.pfoto = async (el) => {
  const f = el.files && el.files[0]; if (!f || !S.assets) return;
  try {
    toast('Enviando foto…', 5000);
    const r = await S.assets.upload(f);
    S.ui.pdraft.foto = r.id; toast('Foto anexada. Salve o produto.'); render();
  } catch (e) { toast(e && e.code === 'too_large' ? 'Foto muito grande (máx. 20 MB).' : 'Não foi possível enviar a foto.', 4000); }
};
ACT.rmFoto = () => { S.ui.pdraft.foto = null; render(); };
ACT.cancelProd = () => { S.ui.pdraft = null; go('#/cad'); };
ACT.salvarProd = async (el) => {
  if (S.saving) return;
  const d = S.ui.pdraft; const errs = [];
  const codigo = String(d.codigo || '').trim();
  if (!codigo) errs.push('Informe o código.');
  else if (!/^[A-Za-z0-9_.\-]+$/.test(codigo) || codigo === '.' || codigo === '..') errs.push('Código deve ter apenas letras, números, ponto, hífen ou sublinhado.');
  else if (d._novo && S.produtos.has(codigo)) errs.push(`Já existe um produto com o código ${codigo}.`);
  if (!String(d.descricao || '').trim()) errs.push('Informe a descrição.');
  if (!d.variantes.length) errs.push('Cadastre pelo menos uma variante.');
  const nomes = new Set();
  const vars = d.variantes.map((v, ix) => {
    const o = { id: v.id, nome: String(v.nome || '').trim() || `Variante ${ix + 1}`, embalagem: v.embalagem, metodo: 'B', densidade: null, condPesagem: String(v.condPesagem || '').trim(), ativo: v.ativo !== false, notas: v.notas || {} };
    if (nomes.has(o.nome)) errs.push(`Duas variantes com o nome "${o.nome}".`); nomes.add(o.nome);
    for (const [k, l] of NUMF) {
      const pr = (k === 'densidade' ? parseD : parseF)(v['_' + k]);
      if (!pr.ok) errs.push(`Variante ${ix + 1} — ${l}: ${pr.error}`);
      o[k] = pr.ok ? pr.value : null;
      if (pr.ok && pr.value === 0 && k !== 'taraG') errs.push(`Variante ${ix + 1} — ${l} não pode ser zero.`);
    }
    return o;
  });
  if (errs.length) { await modal({ title: 'Revise o cadastro', body: `<div class="note n-err">${errs.map(esc).join('<br>')}</div>`, actions: [{ label: 'Voltar', value: true, cls: 'pri' }] }); return; }
  const doc = { codigo, descricao: d.descricao.trim(), categoria: String(d.categoria || '').trim().toUpperCase() || Defaults.categoriaPara(d.descricao), marca: String(d.marca || '').trim(), unidade: d.unidade, forma: d.unidade === 'UNID' ? 'fechadas' : d.forma,
    tipo: d.tipo || 'INSUMO', marcas: d.marcas || [], conferir: d.conferir || [],
    amb: d.amb || {}, foto: d.foto || null, fotoMini: d.fotoMini || null, fotoUrl: d.fotoUrl || '', obs: String(d.obs || '').trim(), ativo: d.ativo !== false, origem: d.origem || (d._novo ? 'Cadastro manual' : ''),
    variantes: vars, criadoEm: d.criadoEm || Date.now(), atualizadoEm: Date.now(), atualizadoPor: S.me || '' };
  S.saving = true; el.disabled = true;
  try {
    await comSync(() => S.db.doc(`produtos/${codigo}`).set(doc));
    S.ui.pdraft = null; toast('Produto salvo ✓'); go('#/cad');
  } catch (e) { toast(errMsg(e), 4000); }
  finally { S.saving = false; el.disabled = false; }
};

/* ---- Ambientes ---- */
function viewAmbientes() {
  const todos = [...S.ambientes].sort((a, b) => (a.ordem || 0) - (b.ordem || 0));
  return `<div class="stack" style="margin-top:14px">
    <button class="btn big pri full" data-act="go" data-to="#/cad/a/novo">Novo ambiente</button>
    <div class="list">${todos.map((a) => {
      const n = [...S.produtos.values()].filter((p) => p.ativo !== false && p.amb && p.amb[a.id]).length;
      const nab = [...S.produtos.values()].filter((p) => p.ativo !== false && p.amb && p.amb[a.id] && p.amb[a.id].abertas && p.forma === 'fechadas_abertas').length;
      return `<button class="li" data-act="go" data-to="#/cad/a/${a.id}"><span class="grow"><span class="nm">${esc(a.nome)}</span><br><span class="meta">${n} produtos</span></span>${a.ativo === false ? '<span class="pill p-pend">Inativo</span>' : '<span class="pill p-ok">Ativo</span>'}</button>`;
    }).join('')}</div></div>`;
}
VIEWS.ambiente = (r) => {
  const novo = r.id === 'novo';
  const a = novo ? { nome: '', ativo: true, ordem: S.ambientes.length + 1 } : S.ambientes.find((x) => x.id === r.id);
  if (!a) return `${topbar({ back: '#/cad/amb', title: 'Ambiente não encontrado' })}`;
  const prods = [...S.produtos.values()].filter((p) => p.ativo !== false);
  const cats = categorias();
  return `${topbar({ back: '#/cad/amb', eyebrow: novo ? 'Novo ambiente' : 'Ambiente', title: esc(a.nome || 'Novo ambiente') })}
  <div class="stack" style="margin-top:14px">
    <section class="card stack">
      <label class="f">Nome<input id="a-nome" value="${esc(a.nome)}"></label>
      <label class="f">Ordem de exibição<input id="a-ordem" inputmode="numeric" value="${esc(a.ordem || '')}"></label>
      <label class="check"><input type="checkbox" id="a-ativo" ${a.ativo !== false ? 'checked' : ''}> Ativo (aparece em novas contagens)</label>
      <button class="btn big pri full" data-act="salvarAmb" data-id="${novo ? '' : a.id}">Salvar ambiente</button>
    </section>
    ${novo ? '<p class="small muted">Depois de salvar, volte aqui para escolher os produtos contados neste ambiente.</p>' : `
    <section class="card stack">
      <h3>Produtos contados aqui, por categoria</h3>
      <p class="small muted" style="margin:0">Tire daqui o que nunca fica neste local (ex.: sorvete fora da câmara fria) para a lista ficar mais curta. Para produtos individuais, use o cadastro do produto.</p>
      <div class="tw"><table><thead><tr><th>Categoria</th><th class="n">Incluídos</th><th>Ações</th></tr></thead><tbody>
      ${cats.map((c) => {
        const ps = prods.filter((p) => (p.categoria || 'SEM CATEGORIA') === c);
        const inc = ps.filter((p) => p.amb && p.amb[a.id]).length;
        const ab = ps.filter((p) => p.forma === 'fechadas_abertas' && p.amb && p.amb[a.id] && p.amb[a.id].abertas).length;
        const podeAb = ps.filter((p) => p.forma === 'fechadas_abertas').length;
        return `<tr><td>${esc(c)}<br><span class="tiny muted">${podeAb ? `pesam abertas: ${ab}/${podeAb}` : ''}</span></td><td class="n">${inc}/${ps.length}</td>
          <td><div class="row" style="gap:6px">
            <button class="btn small" style="min-height:38px" data-act="ambBloco" data-a="${a.id}" data-c="${esc(c)}" data-op="${inc === ps.length ? 'rm' : 'add'}">${inc === ps.length ? 'Remover' : 'Incluir todos'}</button>
            ${podeAb && inc ? `<button class="btn small" style="min-height:38px" data-act="ambBloco" data-a="${a.id}" data-c="${esc(c)}" data-op="${ab ? 'abOff' : 'abOn'}">${ab ? 'Sem pesar abertas' : 'Pesar abertas'}</button>` : ''}
          </div></td></tr>`;
      }).join('')}</tbody></table></div>
    </section>`}
  </div>`;
};
ACT.salvarAmb = async (el) => {
  const nome = $('#a-nome').value.trim(); if (!nome) return toast('Informe o nome');
  const ordem = Number($('#a-ordem').value) || S.ambientes.length + 1;
  const ativo = $('#a-ativo').checked;
  let id = el.dataset.id;
  if (!id) {
    id = nome.normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || uid('amb');
    if (S.ambientes.find((x) => x.id === id)) id += '-' + Math.random().toString(36).slice(2, 5);
  }
  try {
    await comSync(() => S.db.doc(`ambientes/${id}`).set({ nome, ordem, ativo }));
    toast('Ambiente salvo ✓'); go(`#/cad/a/${id}`);
  } catch (e) { toast(errMsg(e), 4000); }
};
ACT.ambBloco = async (el) => {
  if (S.saving) return;
  const a = el.dataset.a, c = el.dataset.c, op = el.dataset.op;
  const ps = [...S.produtos.values()].filter((p) => p.ativo !== false && (p.categoria || 'SEM CATEGORIA') === c);
  const alvo = ps.filter((p) => {
    const tem = p.amb && p.amb[a];
    if (op === 'add') return !tem; if (op === 'rm') return tem;
    if (op === 'abOn') return tem && p.forma === 'fechadas_abertas' && !p.amb[a].abertas;
    if (op === 'abOff') return tem && p.forma === 'fechadas_abertas' && p.amb[a].abertas;
    return false;
  });
  if (!alvo.length) return;
  if (op === 'rm' && !(await confirmar('Remover do ambiente?', `${alvo.length} produtos de ${esc(c)} deixarão de aparecer neste ambiente nas próximas contagens.`, 'Remover', 'dan'))) return;
  S.saving = true; el.disabled = true;
  try {
    await comSync(async () => {
      let i = 0;
      for (const p of alvo) {
        const amb = { ...(p.amb || {}) };
        if (op === 'add') amb[a] = { abertas: true };
        else if (op === 'rm') delete amb[a];
        else amb[a] = { abertas: op === 'abOn' };
        await S.db.doc(`produtos/${p.codigo}`).update({ amb, atualizadoEm: Date.now() });
        if (++i % 10 === 0) toast(`Atualizando… ${i}/${alvo.length}`, 1500);
      }
    });
    toast(`${alvo.length} produtos atualizados ✓`);
  } catch (e) { toast(errMsg(e), 4000); }
  finally { S.saving = false; render(); }
};

/* ---- Importação ---- */
function viewImportar() {
  const im = S.ui.impr;
  let prev = '';
  if (im && im.erro) prev = `<div class="note n-err">${esc(im.erro)}</div>`;
  else if (im) {
    const novos = im.validos.filter((x) => !S.produtos.has(x.codigo));
    const exist = im.validos.filter((x) => S.produtos.has(x.codigo));
    const dif = exist.filter((x) => { const p = S.produtos.get(x.codigo); return p.descricao !== x.descricao || (p.categoria || '') !== x.categoria || p.unidade !== x.unidade; });
    prev = `<section class="card stack lift">
      <h3>Prévia — ${esc(im.arquivo)}</h3>
      <div class="small muted">Aba “${esc(im.aba)}”, cabeçalho na linha ${im.linhaCabecalho}. Títulos de grupo e linhas vazias foram ignorados. As colunas de contagem não são importadas (vazio não vira estoque zero).</div>
      <div class="grid2">
        <div><div class="eyebrow">Novos</div><b class="num" style="font-size:24px">${novos.length}</b></div>
        <div><div class="eyebrow">Já cadastrados</div><b class="num" style="font-size:24px">${exist.length}</b> <span class="small muted">(${dif.length} com diferença)</span></div>
        <div><div class="eyebrow">Ignorados / inválidos</div><b class="num" style="font-size:24px">${im.ignorados.length + im.invalidos.length}</b></div>
        <div><div class="eyebrow">Códigos repetidos</div><b class="num" style="font-size:24px">${im.duplicados.length}</b></div>
      </div>
      ${[...im.invalidos, ...im.duplicados, ...im.ignorados].length ? `<details><summary class="small">Ver linhas não importadas</summary><div class="small stack" style="margin-top:6px;gap:4px">${[...im.invalidos, ...im.duplicados, ...im.ignorados].map((x) => `<div>Linha ${x.linha}: ${esc(x.descricao || '')} — ${esc(x.motivo)}</div>`).join('')}</div></details>` : ''}
      ${novos.length ? `<details open><summary class="small"><b>Novos produtos</b></summary><div class="tw" style="margin-top:6px;max-height:320px"><table><thead><tr><th>Código</th><th>Descrição</th><th>Categoria</th><th>UM</th><th>Forma sugerida</th></tr></thead><tbody>
        ${novos.map((x) => `<tr><td>${esc(x.codigo)}</td><td>${esc(x.descricao)}${x.conferir && x.conferir.length ? `<br><span class="tiny" style="color:var(--amber)">${x.conferir.map(esc).join(' · ')}</span>` : ''}</td><td>${esc(x.categoria)}</td><td>${x.unidade}</td><td class="small">${FORMA_LABEL[Defaults.formaPara(x.categoria, x.unidade, x.descricao)]}</td></tr>`).join('')}</tbody></table></div></details>` : ''}
      ${dif.length ? `<details><summary class="small"><b>Diferenças em produtos já cadastrados</b></summary><div class="tw" style="margin-top:6px;max-height:320px"><table><thead><tr><th>Código</th><th>No app</th><th>Na planilha</th></tr></thead><tbody>
        ${dif.map((x) => { const p = S.produtos.get(x.codigo); return `<tr><td>${esc(x.codigo)}</td><td>${esc(p.descricao)}<br><span class="tiny muted">${esc(p.categoria)} · ${p.unidade}</span></td><td>${esc(x.descricao)}<br><span class="tiny muted">${esc(x.categoria)} · ${x.unidade}</span></td></tr>`; }).join('')}</tbody></table></div></details>
        <label class="check"><input type="checkbox" id="imp-upd" data-ch="impUpd" ${S.ui.impUpd ? 'checked' : ''}> Atualizar descrição, categoria e unidade dos ${dif.length} já cadastrados (taras, pesos e locais são mantidos)</label>` : ''}
      <p class="small muted" style="margin:0">Produtos novos entram em todos os locais ativos. Pesos de porção que estão no nome (ex.: 160G) entram para conferir.</p>
      <button class="btn big pri full" data-act="importar" ${novos.length || (dif.length && S.ui.impUpd) ? '' : 'disabled'}>Importar ${novos.length} novos${dif.length && S.ui.impUpd ? ` e atualizar ${dif.length}` : ''}</button>
      <button class="btn ghost" data-act="impLimpar">Cancelar</button>
    </section>`;
  }
  return `<div class="stack" style="margin-top:14px">
    <p class="small muted" style="margin:0">Use a planilha no formato da “CONTAGEM CÂMARAS” (Item, Descrição Item, UM). Produtos são identificados pelo código — nada é duplicado. Itens sem código ganham um código provisório (SC-…).</p>
    <label class="f">Arquivo .xlsx<input type="file" id="imp-file" accept=".xlsx,.xls" data-ch="impFile"></label>
    ${prev}
  </div>`;
}
INP.impFile = async (el) => {
  const f = el.files && el.files[0]; if (!f) return;
  try {
    toast('Lendo a planilha…', 2500);
    await carregarLib('XLSX');
    const wb = XLSX.read(await f.arrayBuffer(), { type: 'array' });
    let res = null, aba = null;
    const ordem = [...wb.SheetNames].sort((a, b) => (Defaults.norm(b) === 'IMPRIMIR') - (Defaults.norm(a) === 'IMPRIMIR'));
    for (const n of ordem) {
      const rows = XLSX.utils.sheet_to_json(wb.Sheets[n], { header: 1, raw: true, defval: null, blankrows: true });
      const r = Defaults.lerLinhas(rows);
      if (!r.erro) { res = r; aba = n; break; }
      res = r;
    }
    S.ui.impr = { ...res, arquivo: f.name, aba };
  } catch (e) { S.ui.impr = { erro: 'Não foi possível ler o arquivo: ' + (e.message || e) }; }
  S.ui.impUpd = false; render();
};
INP.impUpd = (el) => { S.ui.impUpd = el.checked; render(); };
ACT.impLimpar = () => { S.ui.impr = null; render(); };
ACT.importar = async (el) => {
  if (S.saving) return;
  const im = S.ui.impr;
  const novos = im.validos.filter((x) => !S.produtos.has(x.codigo));
  const dif = S.ui.impUpd ? im.validos.filter((x) => { const p = S.produtos.get(x.codigo); return p && (p.descricao !== x.descricao || (p.categoria || '') !== x.categoria || p.unidade !== x.unidade); }) : [];
  S.saving = true; el.disabled = true;
  let n = 0;
  try {
    await comSync(async () => {
      const ativos = ambAtivos().map((a) => a.id);
      for (const x of novos) {
        const p = Defaults.novoProduto(x, im.arquivo);
        p.amb = Object.fromEntries(Object.entries(p.amb).filter(([k]) => ativos.includes(k)));
        await S.db.doc(`produtos/${p.codigo}`).set(p);
        if (++n % 20 === 0) { el.textContent = `Importando… ${n}/${novos.length + dif.length}`; }
      }
      for (const x of dif) {
        await S.db.doc(`produtos/${x.codigo}`).update({ descricao: x.descricao, categoria: x.categoria, unidade: x.unidade, atualizadoEm: Date.now() });
        n++;
      }
    });
    toast(`Importação concluída: ${novos.length} novos, ${dif.length} atualizados ✓`, 3500);
    S.ui.impr = null; go('#/cad');
  } catch (e) { toast(`${errMsg(e)} (${n} gravados antes do erro)`, 5000); render(); }
  finally { S.saving = false; }
};

/* ---- Recipientes (taras) e limites de divergência ---- */
function viewAjustes() {
  const rs = [...(S.recipientes || [])].sort((a, b) => collator.compare(a.nome || '', b.nome || ''));
  const c = S.config || {};
  return `<div class="stack" style="margin-top:14px">
    <section class="card stack">
      <h3>Recipientes</h3>
      <p class="small muted" style="margin:0">Na hora de pesar, o contador escolhe o recipiente e o app desconta a tara. Pese cada recipiente vazio (com tampa, se pesam com tampa) e cadastre aqui.</p>
      <div class="list">${rs.length ? rs.map((r) => `<button class="li" data-act="editRecip" data-id="${esc(r.id)}"><span class="grow"><span class="nm">${esc(r.nome)}</span><br><span class="meta">${F(r.taraG)} g</span></span>${r.ativo === false ? '<span class="pill p-pend">Inativo</span>' : ''}</button>`).join('') : '<div class="li muted" style="cursor:default">Nenhum recipiente cadastrado ainda.</div>'}</div>
      <button class="btn big pri full" data-act="editRecip" data-id="">+ Novo recipiente</button>
    </section>
    <section class="card stack">
      <h3>Divergência Domingo × Segunda</h3>
      <p class="small muted" style="margin:0">Um item fica marcado ⚠ quando a diferença passa de qualquer um destes limites.</p>
      <div class="grid2">
        <label class="f">Diferença em kg<div class="unitwrap"><input id="lim-kg" inputmode="decimal" value="${esc(numIn(c.limKg ?? 0.5))}"><span class="u">kg</span></div></label>
        <label class="f">Diferença em unidades<div class="unitwrap"><input id="lim-un" inputmode="numeric" value="${esc(String(c.limUn ?? 2))}"><span class="u">un.</span></div></label>
        <label class="f">Diferença em %<div class="unitwrap"><input id="lim-pct" inputmode="decimal" value="${esc(numIn(c.limPct ?? 10))}"><span class="u">%</span></div></label>
      </div>
      <button class="btn big full" data-act="salvarLimites">Salvar limites</button>
    </section>
  </div>`;
}
ACT.editRecip = async (el) => {
  const r = (S.recipientes || []).find((x) => x.id === el.dataset.id) || null;
  const v = await modal({
    title: r ? 'Recipiente' : 'Novo recipiente',
    body: `<label class="f">Nome<input id="r-nome" value="${esc(r ? r.nome : '')}" placeholder="ex.: GN 1/3 inox com tampa"></label>
      <label class="f">Peso vazio (tara)<div class="unitwrap"><input id="r-tara" inputmode="decimal" value="${esc(r ? numIn(r.taraG) : '')}" placeholder="ex.: 480"><span class="u">g</span></div></label>
      ${r ? `<label class="check"><input type="checkbox" id="r-ativo" ${r.ativo !== false ? 'checked' : ''}> Ativo (aparece na lista)</label>` : ''}`,
    actions: [{ label: 'Salvar', cls: 'pri',
      check: (m) => { const t = Calc.parseNum(m.querySelector('#r-tara').value, { thousandsDot: true });
        if (!m.querySelector('#r-nome').value.trim()) { toast('Informe o nome'); return false; }
        if (!t.ok || t.empty) { toast(t.error || 'Informe o peso vazio em gramas'); return false; }
        return true; },
      collect: (m) => ({ nome: m.querySelector('#r-nome').value.trim(), taraG: Calc.parseNum(m.querySelector('#r-tara').value, { thousandsDot: true }).value,
        ativo: m.querySelector('#r-ativo') ? m.querySelector('#r-ativo').checked : true }) },
      ...(r ? [{ label: 'Excluir', cls: 'dan', value: { excluir: true } }] : []),
      { label: 'Cancelar', value: null, cls: 'ghost' }],
  });
  if (!v) return;
  try {
    if (v.excluir) await comSync(() => S.db.doc(`recipientes/${r.id}`).delete());
    else await comSync(() => S.db.doc(`recipientes/${r ? r.id : uid('r')}`).set({ ...v, atualizadoEm: Date.now() }));
    toast(v.excluir ? 'Recipiente excluído' : 'Recipiente salvo ✓'); render();
  } catch (e) { toast(errMsg(e), 4000); }
};
ACT.salvarLimites = async () => {
  const kg = Calc.parseNum($('#lim-kg').value), un = Calc.parseNum($('#lim-un').value), pct = Calc.parseNum($('#lim-pct').value);
  if (!kg.ok || kg.empty || !un.ok || un.empty || !pct.ok || pct.empty) return toast('Preencha os três limites com números');
  try {
    await comSync(() => S.db.doc('config/geral').set({ limKg: kg.value, limUn: un.value, limPct: pct.value, atualizadoEm: Date.now(), por: S.me || '' }));
    S.config = { ...S.config, limKg: kg.value, limUn: un.value, limPct: pct.value };
    toast('Limites salvos ✓');
  } catch (e) { toast(errMsg(e), 4000); }
};

/* ===== Levar o cadastro de um app para o outro por arquivo ===== */
function cartaoMigrar() {
  if (typeof CFG !== 'undefined' || !ehGestor()) return '';
  return `<section class="card stack">
    <h3>Levar o cadastro para o app do celular</h3>
    <div class="small muted">Baixa um arquivo com os <b>${S.produtos.size} produtos</b> (fotos, taras, volumes) e os ambientes. Depois, no app do celular, use <b>Cadastro › Importar</b> e escolha esse arquivo.</div>
    <button class="btn full" data-act="baixarCad" id="b-bxcad">Baixar cadastro (arquivo)</button>
  </section>`;
}
function cadastroEmArquivo() {
  return {
    formato: 'contagem-amauri/cadastro',
    versao: 1,
    geradoEm: new Date().toISOString(),
    produtos: [...S.produtos.entries()].map(([id, p]) => ({ ...p, codigo: p.codigo || id })),
    ambientes: S.ambientes.map((a) => ({ id: a.id, nome: a.nome, ordem: a.ordem, ativo: a.ativo !== false })),
  };
}
ACT.baixarCad = async (el) => {
  if (!S.dl) return toast('Download indisponível nesta visualização.', 4000);
  el.disabled = true; el.textContent = 'Preparando…';
  try {
    const txt = JSON.stringify(cadastroEmArquivo());
    const nome = `cadastro-amauri-${new Date().toISOString().slice(0, 10)}.json`;
    await S.dl.save({ filename: nome, data: new Blob([txt], { type: 'application/json' }) });
    toast(`Arquivo gerado: ${nome} ✓`, 4000);
  } catch (e) {
    if (e && e.code === 'declined') toast('Download cancelado');
    else toast('Não consegui gerar o arquivo: ' + ((e && e.message) || e), 5000);
  } finally { el.disabled = false; el.textContent = 'Baixar cadastro (arquivo)'; }
};

/* ---- Importar esse arquivo (no app do celular) ---- */
INP.impCad = async (el) => {
  const f = el.files && el.files[0];
  if (!f) return;
  let dados;
  try { dados = JSON.parse(await f.text()); } catch (e) { return toast('Arquivo inválido — use o arquivo gerado pelo outro app.', 5000); }
  if (!dados || dados.formato !== 'contagem-amauri/cadastro' || !Array.isArray(dados.produtos)) return toast('Esse arquivo não é um cadastro do Contagem Amauri.', 5000);
  const ok = await confirmar('Importar o cadastro?',
    `${dados.produtos.length} produtos e ${(dados.ambientes || []).length} ambientes do arquivo vão substituir o cadastro atual deste app (fotos e taras inclusive). As contagens não são afetadas.`, 'Importar');
  if (!ok) return;
  const btn = $('#b-impcad');
  if (btn) { btn.disabled = true; }
  let n = 0;
  try {
    for (const a of dados.ambientes || []) {
      await S.db.doc(`ambientes/${a.id}`).set({ nome: a.nome, ordem: a.ordem || 0, ativo: a.ativo !== false });
    }
    for (const p of dados.produtos) {
      if (!p || !p.codigo) continue;
      await S.db.doc(`produtos/${p.codigo}`).set(p);
      if (++n % 10 === 0) toast(`Importando… ${n}/${dados.produtos.length}`, 1200);
    }
    toast(`Cadastro importado: ${n} produtos ✓`, 4000);
    go('#/cad');
  } catch (e) { toast(`${errMsg(e)} (${n} gravados antes do erro)`, 6000); }
  finally { if (btn) btn.disabled = false; }
};

/* ===== Acesso: usuários, PIN e níveis ===== */
const PAPEL_LABEL = { gestor: 'Gestor (cadastro e fechamento)', contador: 'Contador (lança a contagem)' };
const ehGestor = () => !!(S.user && S.user.papel === 'gestor');
const usuariosAtivos = () => S.usuarios.filter((u) => u.ativo !== false).sort((a, b) => collator.compare(a.nome, b.nome));

async function hashPin(id, pin) {
  const dados = new TextEncoder().encode(`amauri:${id}:${String(pin)}`);
  const h = await crypto.subtle.digest('SHA-256', dados);
  return [...new Uint8Array(h)].map((b) => b.toString(16).padStart(2, '0')).join('');
}
function sessaoValida() {
  if (!S.user) return false;
  const u = S.usuarios.find((x) => x.id === S.user.id);
  if (!u || u.ativo === false) return false;
  if (u.nome !== S.user.nome || u.papel !== S.user.papel) { S.user = { id: u.id, nome: u.nome, papel: u.papel }; LS.set('sessao', S.user); }
  return true;
}
function entrar(u) {
  S.user = { id: u.id, nome: u.nome, papel: u.papel };
  LS.set('sessao', S.user);
  S.me = u.nome; LS.set('contador', u.nome);
  S.ui.loginId = null; S.ui.pin = '';
}
ACT.sair = async () => {
  if (!(await confirmar('Sair do app?', 'Os lançamentos já salvos ficam guardados. Você precisará do seu PIN para entrar de novo.', 'Sair'))) return;
  S.user = null; LS.del('sessao'); S.ui.loginId = null; S.ui.pin = '';
  go('#/');
};
function barraUsuario() {
  if (!S.user) return '';
  return `<div class="row between small" style="margin-top:6px">
    <span class="muted">Você: <b>${esc(S.user.nome)}</b> · ${S.user.papel === 'gestor' ? 'gestor' : 'contador'}</span>
    <button class="btn ghost" style="min-height:36px;padding:0 12px" data-act="sair">Sair</button></div>`;
}
function semPermissao(titulo) {
  return `${topbar({ back: '#/', eyebrow: 'Acesso restrito', title: titulo })}
  <div class="stack" style="margin-top:16px">
    <div class="note n-warn">Esta parte é do gestor. Peça a quem cuida do cadastro, ou entre com um acesso de gestor.</div>
    <button class="btn big full" data-act="sair">Entrar com outro usuário</button>
  </div>`;
}

/* ---- Primeiro acesso: cria o gestor ---- */
VIEWS.primeiroAcesso = () => `<div class="hero-brand">
    <img class="logo" src="${LOGO}" alt="Mercearia Amauri">
    <div class="deco">Mercearia Amauri · São Paulo</div>
    <h1>Primeiro acesso</h1>
  </div>
  <div class="stack" style="margin-top:18px">
    <div class="note n-info">Crie o acesso do gestor. Depois dá para cadastrar a equipe em Cadastro › Equipe.</div>
    <label class="f">Seu nome<input id="pa-nome" autocomplete="name" placeholder="Ex.: Luiz"></label>
    <label class="f">PIN de 4 dígitos<input id="pa-pin" inputmode="numeric" maxlength="4" autocomplete="off" placeholder="••••"></label>
    <label class="f">Repita o PIN<input id="pa-pin2" inputmode="numeric" maxlength="4" autocomplete="off" placeholder="••••"></label>
    <button class="btn big pri full" data-act="criarGestor" id="b-pa">Criar acesso</button>
    <p class="tiny muted" style="margin:0">O PIN serve para identificar quem lança cada contagem e separar o que só o gestor pode fazer. Ele não substitui o controle de quem recebe o link do app.</p>
  </div>`;
ACT.criarGestor = async (btn) => {
  const nome = $('#pa-nome').value.trim();
  const pin = $('#pa-pin').value.trim(), pin2 = $('#pa-pin2').value.trim();
  if (nome.length < 2) return toast('Informe seu nome');
  if (!/^\d{4}$/.test(pin)) return toast('O PIN precisa ter 4 dígitos');
  if (pin !== pin2) return toast('Os dois PINs estão diferentes');
  if (S.saving) return;
  S.saving = true; btn.disabled = true;
  const id = 'u' + Date.now().toString(36);
  try {
    const hash = await hashPin(id, pin);
    await comSync(() => S.db.doc(`usuarios/${id}`).set({ nome, papel: 'gestor', ativo: true, pin: hash, criadoEm: Date.now() }));
    entrar({ id, nome, papel: 'gestor' });
    toast(`Bem-vindo, ${nome}`);
    render();
  } catch (e) { toast(errMsg(e), 4000); }
  finally { S.saving = false; btn.disabled = false; }
};

/* ---- Login ---- */
VIEWS.login = () => {
  const lista = usuariosAtivos();
  const sel = lista.find((u) => u.id === S.ui.loginId);
  const pin = S.ui.pin || '';
  return `<div class="hero-brand">
    <img class="logo" src="${LOGO}" alt="Mercearia Amauri">
    <div class="deco">Mercearia Amauri · São Paulo</div>
    <h1>Contagem de comida</h1>
  </div>
  <div class="stack" style="margin-top:18px">
    ${!sel ? `<div class="eyebrow">Quem está entrando?</div>
      <div class="list">${lista.map((u) => `<button class="li" data-act="loginSel" data-id="${esc(u.id)}">
        <span class="grow"><span class="nm">${esc(u.nome)}</span><br><span class="meta">${u.papel === 'gestor' ? 'Gestor' : 'Contador'}</span></span><span class="muted">›</span></button>`).join('')}</div>`
    : `<div class="row"><button class="back" data-act="loginVoltar" aria-label="Voltar">‹</button><div><div class="eyebrow">Entrando como</div><h2>${esc(sel.nome)}</h2></div></div>
      <div class="pindots">${[0, 1, 2, 3].map((i) => `<i class="${pin.length > i ? 'on' : ''}"></i>`).join('')}</div>
      <div class="pinpad">
        ${[1, 2, 3, 4, 5, 6, 7, 8, 9].map((n) => `<button data-act="pinDig" data-n="${n}">${n}</button>`).join('')}
        <button class="ghost" data-act="pinApaga" aria-label="Apagar">⌫</button>
        <button data-act="pinDig" data-n="0">0</button>
        <button class="ghost" data-act="loginVoltar">Trocar</button>
      </div>`}
  </div>`;
};
ACT.loginSel = (el) => { S.ui.loginId = el.dataset.id; S.ui.pin = ''; render(); };
ACT.loginVoltar = () => { S.ui.loginId = null; S.ui.pin = ''; render(); };
ACT.pinApaga = () => { S.ui.pin = (S.ui.pin || '').slice(0, -1); render(); };
ACT.pinDig = async (el) => {
  S.ui.pin = ((S.ui.pin || '') + el.dataset.n).slice(0, 4);
  render();
  if (S.ui.pin.length === 4) {
    const u = S.usuarios.find((x) => x.id === S.ui.loginId);
    const hash = await hashPin(u.id, S.ui.pin);
    if (u.pin && hash === u.pin) { entrar(u); toast(`Olá, ${u.nome}`); render(); }
    else { S.ui.pin = ''; toast('PIN incorreto', 2500); render(); }
  }
};

/* ---- Equipe (cadastro) ---- */
function viewEquipe() {
  if (!ehGestor()) return '<div class="note n-warn" style="margin-top:14px">Só o gestor cadastra a equipe.</div>';
  const lista = [...S.usuarios].sort((a, b) => collator.compare(a.nome, b.nome));
  return `<div class="stack" style="margin-top:14px">
    <button class="btn big pri full" data-act="novoUsuario">Nova pessoa</button>
    <div class="list">${lista.map((u) => `<button class="li" data-act="editUsuario" data-id="${esc(u.id)}">
      <span class="grow"><span class="nm">${esc(u.nome)}</span><br><span class="meta">${u.papel === 'gestor' ? 'Gestor' : 'Contador'}${u.pin ? '' : ' · sem PIN'}</span></span>
      ${u.ativo === false ? '<span class="pill p-pend">Inativo</span>' : '<span class="pill p-ok">Ativo</span>'}</button>`).join('')}</div>
    <div class="note n-info small">O gestor cadastra produtos e ambientes, inicia, finaliza e exclui contagens. O contador lança as quantidades e as pesagens.</div>
  </div>`;
}
async function formUsuario(u) {
  const novo = !u;
  const r = await modal({
    title: novo ? 'Nova pessoa' : esc(u.nome),
    body: `<label class="f">Nome<input id="u-nome" value="${esc(novo ? '' : u.nome)}" autocomplete="off"></label>
      <label class="f">Nível<select id="u-papel">
        <option value="contador" ${!novo && u.papel === 'contador' ? 'selected' : ''}>Contador — lança a contagem</option>
        <option value="gestor" ${!novo && u.papel === 'gestor' ? 'selected' : ''}>Gestor — cadastro, início e fechamento</option>
      </select></label>
      <label class="f">${novo ? 'PIN de 4 dígitos' : 'Novo PIN (deixe vazio para manter)'}<input id="u-pin" inputmode="numeric" maxlength="4" autocomplete="off" placeholder="••••"></label>
      <label class="check"><input type="checkbox" id="u-ativo" ${novo || u.ativo !== false ? 'checked' : ''}> Ativo</label>`,
    actions: [{ label: 'Salvar', cls: 'pri', collect: (m) => ({
      nome: m.querySelector('#u-nome').value.trim(), papel: m.querySelector('#u-papel').value,
      pin: m.querySelector('#u-pin').value.trim(), ativo: m.querySelector('#u-ativo').checked }) },
      ...(novo ? [] : [{ label: 'Excluir pessoa', cls: 'dan', value: { excluir: true } }]),
      { label: 'Cancelar', value: null, cls: 'ghost' }],
  });
  if (!r) return;
  try {
    if (r.excluir) {
      if (!(await confirmar('Excluir pessoa?', `<b>${esc(u.nome)}</b> não poderá mais entrar. Os lançamentos já feitos por ela continuam no histórico.`, 'Excluir', 'dan'))) return;
      if (S.usuarios.filter((x) => x.papel === 'gestor' && x.ativo !== false).length < 2 && u.papel === 'gestor') return toast('Cadastre outro gestor antes de excluir este.', 4000);
      await comSync(() => S.db.doc(`usuarios/${u.id}`).delete());
      toast('Pessoa excluída'); render(); return;
    }
    if (r.nome.length < 2) return toast('Informe o nome');
    if (novo && !/^\d{4}$/.test(r.pin)) return toast('O PIN precisa ter 4 dígitos');
    if (!novo && r.pin && !/^\d{4}$/.test(r.pin)) return toast('O PIN precisa ter 4 dígitos');
    const id = novo ? 'u' + Date.now().toString(36) : u.id;
    const doc = { nome: r.nome, papel: r.papel, ativo: r.ativo, atualizadoEm: Date.now() };
    if (novo || r.pin) doc.pin = await hashPin(id, r.pin);
    else doc.pin = u.pin;
    if (!novo && u.papel === 'gestor' && r.papel !== 'gestor' &&
        S.usuarios.filter((x) => x.papel === 'gestor' && x.ativo !== false).length < 2) return toast('Deixe pelo menos um gestor ativo.', 4000);
    await comSync(() => S.db.doc(`usuarios/${id}`).set({ ...doc, criadoEm: novo ? Date.now() : (u.criadoEm || Date.now()) }));
    toast('Salvo ✓'); render();
  } catch (e) { toast(errMsg(e), 4000); }
}
ACT.novoUsuario = () => formUsuario(null);
ACT.editUsuario = (el) => formUsuario(S.usuarios.find((u) => u.id === el.dataset.id));

/* ===== Envio da planilha para a pasta do Drive ===== */
const DRIVE_PASTA = 'pasta de contagens da Mercearia Amauri';
function cartaoDrive(h) {
  const ex = S.exportacoes[h.id];
  if (ex && ex.status === 'enviado') {
    return `<div class="note n-ok small">Planilha enviada para a pasta <b>${DRIVE_PASTA}</b> no Drive em ${horaBR(ex.enviadoEm)}.
      ${ex.link ? `<br><a href="${esc(ex.link)}" target="_blank" rel="noopener">Abrir no Drive</a>` : ''}</div>
      ${ehGestor() ? '<button class="btn ghost full" data-act="enviarDrive">Enviar de novo</button>' : ''}`;
  }
  if (ex && ex.status === 'pendente') {
    return `<div class="note n-info small">Planilha na fila de envio para o Drive (${horaBR(ex.em)}), por ${esc(ex.por || '—')}. Ela aparece na pasta em até 1 hora.${ex.motivoFila ? `<br>Não foi direto: ${esc(ex.motivoFila)}.` : ''}</div>
      ${ehGestor() ? '<button class="btn full" data-act="enviarDrive">Tentar enviar agora</button>' : ''}`;
  }
  if (ex && ex.status === 'erro') {
    return `<div class="note n-err small">O envio ao Drive falhou: ${esc(ex.erro || 'motivo não informado')}.</div>
      ${ehGestor() ? '<button class="btn warn full" data-act="enviarDrive">Tentar enviar de novo</button>' : ''}`;
  }
  if (!ehGestor()) return '';
  if (!driveAtivo()) return '<div class="note n-info small">Envio para a pasta do Drive indisponível neste aparelho. Baixe a planilha e suba na pasta.</div>';
  return `<button class="btn big full" data-act="enviarDrive" id="btn-drive">Enviar para a pasta do Drive</button>
    <div class="tiny muted">Vai na hora para a pasta <b>${DRIVE_PASTA}</b>, compartilhada com a diretoria.</div>`;
}
const DRIVE_SERVER = 'Google Drive';
const DRIVE_PASTA_ID = '1NQ90CFDJnZasyjyCnO0a4ep6EWTpO4Yv';
const XLSX_MIME = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
let mcpP = null;
const usarMcp = () => (mcpP || (mcpP = (window.claude && window.claude.use ? window.claude.use('mcp') : Promise.resolve(null)).catch(() => null)));
function b64de(bytes) {
  let bin = '';
  for (let i = 0; i < bytes.length; i += 8192) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 8192));
  return btoa(bin);
}
/* Sobe o .xlsx direto na pasta do Drive. Devolve {ok, link} ou {ok:false, code, msg}. */
const DRIVE_WEB = () => (typeof window !== 'undefined' && window.DRIVE_WEBAPP_URL) || '';
const noClaude = () => !!(window.claude && window.claude.use);
const driveAtivo = () => noClaude() || !!DRIVE_WEB();
async function subirViaWebApp(nome, bytes) {
  try {
    const r = await fetch(DRIVE_WEB(), { method: 'POST', headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify({ chave: window.DRIVE_CHAVE || '', nome, base64: b64de(bytes) }) });
    const j = await r.json();
    if (j && j.ok) return { ok: true, link: j.link || `https://drive.google.com/drive/folders/${DRIVE_PASTA_ID}`, id: j.id || '' };
    return { ok: false, code: 'tool_error', msg: (j && j.erro) || 'o Drive recusou o arquivo' };
  } catch (e) {
    return { ok: false, code: 'server_unavailable', msg: 'sem resposta do Drive — confira a internet e a pasta antes de tentar de novo' };
  }
}
async function subirNoDrive(nome, bytes) {
  if (!noClaude() && DRIVE_WEB()) return subirViaWebApp(nome, bytes);
  const mcp = await usarMcp();
  if (!mcp) return { ok: false, code: 'sem_mcp', msg: 'envio direto indisponível nesta visualização' };
  try {
    const r = await mcp.callTool(DRIVE_SERVER, 'create_file', {
      title: nome, parentId: DRIVE_PASTA_ID, contentMimeType: XLSX_MIME,
      base64Content: b64de(bytes), disableConversionToGoogleType: true,
    }, { cache: false });
    const p = (r && r.payload && typeof r.payload === 'object') ? r.payload : {};
    const f = p.file && typeof p.file === 'object' ? p.file : p;
    const link = f.viewUrl || f.webViewLink || f.alternateLink || (f.id ? `https://drive.google.com/file/d/${f.id}/view` : `https://drive.google.com/drive/folders/${DRIVE_PASTA_ID}`);
    return { ok: true, link, id: f.id || '' };
  } catch (e) {
    const code = (e && e.code) || 'upstream_error';
    const msg = code === 'server_not_connected' ? 'o Google Drive não está conectado nesta conta do Claude (Configurações › Conectores)'
      : code === 'needs_reauth' ? 'a conexão com o Google Drive expirou — reconecte em Configurações › Conectores do Claude'
      : code === 'not_in_manifest' || code === 'not_granted' ? 'o envio ao Drive não foi autorizado neste app'
      : code === 'blocked_by_policy' || code === 'approval_required' ? 'o envio ao Drive está bloqueado pela política da conta'
      : code === 'selection_required' ? 'escolha qual conexão do Google Drive usar no aviso do Claude e tente de novo'
      : code === 'tool_error' ? ('o Drive recusou o arquivo: ' + ((e && e.message) || ''))
      : code === 'server_unavailable' ? 'o Google Drive não respondeu — confira a pasta antes de tentar de novo'
      : ((e && e.message) || 'falha no envio');
    return { ok: false, code, msg };
  }
}
/* Gera a planilha e manda na hora; se não der, deixa na fila (tarefa de hora em hora). */
async function enviarContagemDrive(ctx, nomeUser) {
  const h = ctx.h;
  await carregarLib('ExcelJS');
  const { wb, nome } = await gerarWorkbook(ctx, 'todos');
  const bytes = new Uint8Array(await wb.xlsx.writeBuffer());
  const r = await subirNoDrive(nome, bytes);
  if (r.ok) {
    const antigo = S.exportacoes[h.id];
    if (antigo && antigo.partes) { for (let i = 0; i < antigo.partes; i++) { try { await S.db.doc(`exportacoes/${h.id}/p/${i}`).delete(); } catch {} } }
    await comSync(() => S.db.doc(`exportacoes/${h.id}`).set({
      nome, status: 'enviado', enviadoEm: Date.now(), link: r.link, driveId: r.id, por: nomeUser, em: Date.now(),
      data: h.data, responsavel: h.responsavel, pasta: DRIVE_PASTA, direto: true,
    }));
    return { ok: true, link: r.link };
  }
  // quando o Drive pode ter recebido (sem resposta), não duplica pela fila; fora do Claude não há fila
  if (r.code === 'server_unavailable' || r.code === 'upstream_error' || !noClaude()) {
    await comSync(() => S.db.doc(`exportacoes/${h.id}`).set({ nome, status: 'erro', erro: r.msg, por: nomeUser, em: Date.now(), data: h.data, responsavel: h.responsavel, pasta: DRIVE_PASTA }));
    return { ok: false, msg: r.msg };
  }
  const b64 = b64de(bytes);
  const tam = 120000, partes = [];
  for (let i = 0; i < b64.length; i += tam) partes.push(b64.slice(i, i + tam));
  await comSync(async () => {
    for (let i = 0; i < partes.length; i++) await S.db.doc(`exportacoes/${h.id}/p/${i}`).set({ b64: partes[i] });
    await S.db.doc(`exportacoes/${h.id}`).set({ nome, partes: partes.length, bytes: bytes.length, status: 'pendente', por: nomeUser, em: Date.now(),
      data: h.data, responsavel: h.responsavel, pasta: DRIVE_PASTA, motivoFila: r.msg });
  });
  return { ok: false, fila: true, msg: r.msg };
}
ACT.enviarDrive = async (el) => {
  if (!ehGestor()) return toast('Só o gestor envia a planilha para o Drive');
  const ctx = ctxAtual();
  if (S.saving) return;
  S.saving = true; el.disabled = true; const txt = el.textContent; el.textContent = 'Enviando ao Drive…';
  try {
    const r = await enviarContagemDrive(ctx, S.user.nome);
    if (r.ok) toast('Planilha enviada ao Drive ✓', 3500);
    else if (r.fila) toast(`Não foi direto (${r.msg}). Ficou na fila e chega em até 1 hora.`, 7000);
    else toast(`Envio ao Drive falhou: ${r.msg}`, 7000);
    render();
  } catch (e) { console.error(e); toast('Não foi possível preparar o envio: ' + ((e && e.message) || e), 5000); }
  finally { S.saving = false; el.disabled = false; el.textContent = txt; }
};


/* ----- configuração do banco (fica no fim para ACT já existir) ----- */
ACT.abrirConfig = () => { S.semConfig = true; S.noDb = false; render(); };
ACT.salvarConfig = () => {
  const url = $('#cfg-url').value.trim().replace(/\/+$/, ''), key = $('#cfg-key').value.trim();
  if (!/^https:\/\/.+\.supabase\.co$/.test(url)) return toast('A URL deve ser parecida com https://xxxx.supabase.co');
  if (key.length < 30) return toast('Cole a chave anon public inteira');
  CFG.set(url, key);
  location.reload();
};

boot();

