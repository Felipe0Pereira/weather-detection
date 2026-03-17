#!/usr/bin/env bash
#
#   eventos extremos de chuva:
#   RS/2024  — enchentes no Rio Grande do Sul
#   SP/2023  — chuvas extremas em São Paulo
#   PE/2022  — enchentes em Recife/PE
#   RJ/2022  — chuvas no Rio de Janeiro
#   BA/2022  — desastres na Bahia
#   MG/2022  — desastres em Minas Gerais
#
# cada grupo recebe até 5.000 linhas aleatórias (total prioritário = 30.000).
# outras regiões até 20.000 linhas aleatórias.

set -euo pipefail

BASE="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="$BASE/reduced"
FINAL="$OUT_DIR/dataset.csv"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

PRIORITY_ROWS=5000   # linhas por grupo prioritário (6 grupos × 5000 = 30.000)
OTHER_ROWS=20000     # linhas de outras regiões

mkdir -p "$OUT_DIR"

# Extrai linhas de dados de um CSV INMET:
# pula as 9 linhas de metadados/cabeçalho e linhas em branco.
extract() {
    awk 'NR>9 && /[^[:space:]]/' "$1"
}

# Retorna o código de estado (3º campo separado por _) do nome do arquivo.
state_of() {
    basename "$1" | cut -d_ -f3
}

# ── Indexação ─────────────────────────────────────────────────────────────────
echo "Indexando arquivos CSV..."

declare -A prio_files      # chave: "ANO:ESTADO"
declare -a other_files=()

while IFS= read -r -d '' f; do
    year=$(basename "$(dirname "$f")")
    st=$(state_of "$f")
    key="${year}:${st}"
    case "$key" in
        2024:RS|2023:SP|2022:PE|2022:RJ|2022:BA|2022:MG)
            prio_files[$key]+="${f}"$'\n'
            ;;
        *)
            other_files+=("$f")
            ;;
    esac
done < <(find "$BASE/2022" "$BASE/2023" "$BASE/2024" "$BASE/2025" "$BASE/2026" \
         -type f \( -name '*.CSV' -o -name '*.csv' \) -print0)

echo "Grupos prioritários encontrados: ${#prio_files[@]}"
echo "Arquivos de outras regiões: ${#other_files[@]}"

# extrai a linha 9 (nomes das colunas) do primeiro CSV disponível.
first_csv=$(find "$BASE/2022" -type f \( -name '*.CSV' -o -name '*.csv' \) -print -quit)
sed -n '9p' "$first_csv" > "$FINAL"

# ── grupos prioritários ─────────────────────────────────────────
for key in 2024:RS 2023:SP 2022:PE 2022:RJ 2022:BA 2022:MG; do
    if [[ -z "${prio_files[$key]+x}" ]]; then
        echo "AVISO: nenhum arquivo para o grupo '$key', pulando."
        continue
    fi

    pool="$TMP/prio_${key//:/_}.pool"

    # Concatena as linhas de dados de todos os arquivos do grupo.
    while IFS= read -r f; do
        [[ -n "$f" ]] && extract "$f"
    done <<< "${prio_files[$key]}" > "$pool"

    available=$(wc -l < "$pool")
    take=$((available < PRIORITY_ROWS ? available : PRIORITY_ROWS))
    shuf -n "$take" "$pool" >> "$FINAL"
    echo "  [$key]  disponível: $available  →  amostrado: $take"
done

# ── outras regiões ───────────────────────────────────────────────
echo "Coletando amostra de outras regiões..."

pool_other="$TMP/other.pool"
: > "$pool_other"

# embaralha a lista de arquivos.
readarray -d '' -t shuffled_others < <(printf '%s\0' "${other_files[@]}" | shuf -z)

for f in "${shuffled_others[@]}"; do
    cur=$(wc -l < "$pool_other")
    (( cur >= OTHER_ROWS )) && break
    extract "$f" >> "$pool_other"
done

available_other=$(wc -l < "$pool_other")
take_other=$((available_other < OTHER_ROWS ? available_other : OTHER_ROWS))
shuf -n "$take_other" "$pool_other" >> "$FINAL"
echo "  [outros]  disponível: $available_other  →  amostrado: $take_other"

total_data=$(( $(wc -l < "$FINAL") - 1 ))
echo ""
echo "Dataset salvo em:          $FINAL"
echo "Total de linhas de dados:  $total_data"