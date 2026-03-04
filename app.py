"""
App Streamlit — Conciliação SIGE × Mercado Pago
Execute com:  streamlit run app.py
"""
import traceback
from datetime import datetime

import streamlit as st

from reconciliacao import processar

# ── Configuração da página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Conciliação SIGE × Mercado Pago",
    page_icon="📊",
    layout="centered",
)

st.title("📊 Conciliação SIGE × Mercado Pago")
st.markdown(
    "Faça upload das duas planilhas e clique em **Processar** para gerar o arquivo de conciliação."
)

# ── Upload das planilhas ──────────────────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Planilha SIGE")
    sige_file = st.file_uploader(
        "Selecione o arquivo SIGE",
        type=["xlsx", "xls"],
        key="sige",
    )

with col2:
    st.subheader("Planilha Mercado Pago")
    mp_file = st.file_uploader(
        "Selecione o arquivo Mercado Pago",
        type=["xlsx", "xls"],
        key="mp",
    )

st.divider()

# ── Botão de processamento ────────────────────────────────────────────────────
if sige_file and mp_file:
    if st.button("🔄 Processar Conciliação", type="primary", use_container_width=True):
        with st.spinner("Processando… aguarde."):
            try:
                sige_bytes = sige_file.read()
                mp_bytes   = mp_file.read()

                resultado = processar(sige_bytes, mp_bytes)

                hoje     = datetime.now().strftime("%Y%m%d")
                filename = f"conciliacao_{hoje}.xlsx"

                st.success("✅ Conciliação gerada com sucesso!")
                st.download_button(
                    label="⬇️ Baixar planilha de conciliação",
                    data=resultado,
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )

            except Exception as exc:
                st.error(f"❌ Erro ao processar: {exc}")
                with st.expander("Detalhes do erro (para diagnóstico)"):
                    st.code(traceback.format_exc())
else:
    missing = []
    if not sige_file:
        missing.append("SIGE")
    if not mp_file:
        missing.append("Mercado Pago")
    st.info(f"👆 Faltam: **{' e '.join(missing)}**. Faça o upload para continuar.")

# ── Rodapé com lembretes de formatação esperada ───────────────────────────────
with st.expander("ℹ️ Formatos esperados das planilhas"):
    st.markdown(
        """
**Planilha SIGE** — deve conter pelo menos as colunas:
- `CLIENTE` (ou variação como *Nome*, *Nome Cliente*)
- `VALOR` (ou *Valor Total*, *Total*)
- `ID VENDA MERCADO LIVRE` (ou *ID ML*, *ID*)

**Planilha Mercado Pago** — exportação de liberações (reserve-release), com as colunas:
- `DATA`, `DESCRIÇÃO`, `CREDITADO`, `DEBITADO`, `SALDO`
- `CÓDIGO DE REFERÊNCIA`, `ID DO PEDIDO`, `ID DO PACOTE`
- `ID DA OPERAÇÃO NO MERCADO PAGO`

O app aceita tanto `.xlsx` quanto `.xls`.
        """
    )
