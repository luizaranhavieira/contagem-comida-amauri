# Contagem de Comida — Mercearia Amauri

App irmão da Contagem de Bebidas: mesmo visual, mesmo login por PIN e o mesmo banco no Supabase, mas numa
tabela separada (`docs_cozinha`). Os dados das bebidas não são tocados.

## 1. Supabase: criar a tabela (5 minutos)

1. Entre no **mesmo projeto** do app de bebidas em supabase.com.
2. **SQL Editor › New query**: cole o `schema.sql` e clique em **Run**.
3. Nova query: cole o `seed.sql` e clique em **Run**. Isso cria os 3 ambientes e os 106 produtos da planilha
   CONTAGEM CÂMARAS. Pode rodar de novo sem duplicar nada.

O `config.js` já aponta para o mesmo Supabase das bebidas; não precisa mexer.

## 2. GitHub: publicar

1. Crie um repositório novo chamado `contagem-comida-amauri` (público, igual ao de bebidas).
2. **Add file › Upload files**: arraste **todos os arquivos desta pasta** e confirme.
3. **Settings › Pages**: *Deploy from a branch*, branch `main`, pasta `/ (root)`, salvar.
4. Em 1 ou 2 minutos fica no ar em `https://luizaranhavieira.github.io/contagem-comida-amauri/`.

## 3. Primeiro uso

1. Abra o link no celular. O app pede para criar o gestor (nome e PIN de 4 dígitos).
   O login é separado do app de bebidas: crie de novo.
2. **⚙ › Cadastro › Equipe**: cadastre os cozinheiros como *contador*.
3. **Cadastro › Taras**: pese cada recipiente vazio (GN 1/1, 1/2, 1/3, potes) e cadastre.
   Na hora de pesar, o contador escolhe o recipiente e o app desconta a tara.
4. **Cadastro › Produtos › A conferir**: são 13 itens que vieram da planilha com algo a revisar
   (sem código, sem unidade, ou "unidade" no nome com a planilha em KG). Confira também os pesos de porção,
   que vieram do nome (ex.: ENTRECOTE 160G → porção de 160 g).
5. **Cadastro › Ambientes**: tire de cada câmara as categorias que nunca ficam lá
   (ex.: sorvete fora da câmara fria). As listas ficam mais curtas.
6. Instale no celular: Android (Chrome) › ⋮ › *Instalar app*; iPhone (Safari) › compartilhar ›
   *Adicionar à Tela de Início*. O ícone é creme e escrito COMIDA, para não confundir com o de bebidas.

## Como conta

- **Unidades** (coxinha, croquete, burrata): número + setas.
- **Porcionados** com peso no nome (Entrecote 160 g, Salmão 300 g): conta as porções fechadas e,
  se tiver porção aberta ou sobra, pesa no botão da balança. O app transforma tudo em kg.
- **Produção própria e insumos**: botão **Pesar**. Digite o peso da balança em **kg** (ex.: 2,350)
  e escolha o recipiente.
- **Item em branco** num ambiente = não tinha lá. Na planilha ele fica vazio, como na planilha de papel.

## Domingo e segunda

- **Domingo**: contagem principal. O app sugere o tipo pelo dia da semana.
- **Segunda**: o app liga com o domingo anterior. O número do domingo só aparece depois que o
  item é lançado, com a diferença ao lado (⚠ = passou do limite).
- Ao finalizar a segunda, abre a tela **Domingo × Segunda** com os divergentes. A planilha da segunda
  sai com a aba "Domingo x Segunda".
- Os limites (padrão: 0,5 kg, 2 unidades ou 10%) ficam em **Cadastro › Taras**.

## Planilha Excel

A aba **Contagem** usa o layout da CONTAGEM CÂMARAS (Item | Descrição Item | UM | Câmara Fria |
Câmara Congelada | Câmara Fria | TOTAL), com as variantes do mesmo código somadas.
Outras abas: Detalhe por item, Pesagens, Parâmetros usados e Domingo x Segunda.

## Observações

- **Drive**: o envio usa o mesmo Apps Script do app de bebidas, então a planilha cai na mesma pasta
  (o nome do arquivo diz "CONTAGEM COMIDA"). Para uma pasta separada, crie outro Apps Script e troque o
  `DRIVE_WEBAPP_URL` no `config.js`.
- **Internet**: igual ao de bebidas. A contagem grava direto no banco. Sem sinal na câmara, o app avisa que
  não salvou; na tela do item, o lançamento fica guardado como rascunho no aparelho até voltar o sinal.
- **Segurança**: igual ao de bebidas. O PIN separa gestor de contador, e o link deve ficar só com a equipe.
