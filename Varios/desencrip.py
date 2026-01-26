import pandas as pd
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import binascii


ARCHIVO_ENTRADA = r"C:\Users\gcpp.ggi.sge2\Desktop\GGI\df_traza_rebagliati_cut.xlsx"
ARCHIVO_SALIDA = "pacientes_desencriptado_acto_med.xlsx"
COLUMNA = "acto_med"
CLAVE = b"gcpp_ggi_sge_ag\x00"  


BLOCK_SIZE = 16

def decrypt_aes_hex(valor_hex):
    if pd.isna(valor_hex):
        return None
    try:
        data = binascii.unhexlify(valor_hex)
        cipher = AES.new(CLAVE, AES.MODE_ECB)
        decrypted = cipher.decrypt(data)
        decrypted = unpad(decrypted, BLOCK_SIZE)
        return decrypted.decode("utf-8")
    except Exception as e:
        return f"ERROR: {e}"

df = pd.read_excel(ARCHIVO_ENTRADA)

df[COLUMNA + "_original"] = df[COLUMNA].apply(decrypt_aes_hex)

df.to_excel(ARCHIVO_SALIDA, index=False)

print("Archivo generado:", ARCHIVO_SALIDA)
