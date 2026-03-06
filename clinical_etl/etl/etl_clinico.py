import pandas as pd
import json

# =========================================
# CARGAR ARCHIVOS
# =========================================

def cargar_csv(ruta):

    df = pd.read_csv(ruta)

    return df


def cargar_json(ruta):

    with open(ruta,"r",encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    return df


# =========================================
# LIMPIEZA PACIENTES
# =========================================

def procesar_pacientes(df):

    pacientes = df[[
        "record_id",
        "paciente_nombre",
        "documento",
        "sede",
        "anio_nacimiento",
        "fecha_ingreso"
    ]].copy()

    pacientes = pacientes.rename(columns={
        "record_id":"patient_id",
        "sede":"site"
    })

    return pacientes


# =========================================
# LIMPIEZA EVENTOS REDCAP
# =========================================

def procesar_eventos(df):

    # convertir formato largo a ancho
    eventos = df.pivot_table(
        index=["record_id","event_name"],
        columns="field_name",
        values="value",
        aggfunc="first"
    ).reset_index()

    eventos = eventos.rename(columns={
        "record_id":"patient_id",
        "peso":"weight_kg",
        "fecha_evento":"event_date"
    })

    eventos["event_date"] = pd.to_datetime(eventos["event_date"],errors="coerce")
    eventos["weight_kg"] = pd.to_numeric(eventos["weight_kg"],errors="coerce")

    return eventos[[
        "patient_id",
        "event_name",
        "event_date",
        "weight_kg"
    ]]


# =========================================
# ALERTAS DE CALIDAD
# =========================================

def detectar_alertas(df):

    alertas = []

    pacientes = df["patient_id"].unique()

    for p in pacientes:

        temp = df[df["patient_id"]==p]

        basal = temp[temp["event_name"]=="basal"]

        if len(basal)==0:
            continue

        fecha_basal = basal.iloc[0]["event_date"]

        seguimientos = temp[temp["event_name"]!="basal"]

        for _,row in seguimientos.iterrows():

            if row["event_date"] < fecha_basal:

                alertas.append({
                    "patient_id":p,
                    "mensaje":"ERROR: Fecha ilógica"
                })

    return pd.DataFrame(alertas)


# =========================================
# PIPELINE
# =========================================

def ejecutar_pipeline():

    print("Cargando datos...")

    df_admin = cargar_csv(
        r"C:\Users\valer\OneDrive\Desktop\clinical_etl\data\legacy_admin_db.csv"
    )

    df_redcap = cargar_json(
        r"C:\Users\valer\OneDrive\Desktop\clinical_etl\data\redcap_export_raw.json"
    )

    print("Procesando pacientes...")

    pacientes = procesar_pacientes(df_admin)

    print("Procesando eventos clínicos...")

    eventos = procesar_eventos(df_redcap)

    print("Detectando alertas...")

    alertas = detectar_alertas(eventos)

    print("Guardando resultados...")

    pacientes.to_csv(
        r"C:\Users\valer\OneDrive\Desktop\clinical_etl\output\patients_clean.csv",
        index=False
    )

    eventos.to_csv(
        r"C:\Users\valer\OneDrive\Desktop\clinical_etl\output\events_clean.csv",
        index=False
    )

    alertas.to_csv(
        r"C:\Users\valer\OneDrive\Desktop\clinical_etl\output\quality_alerts.csv",
        index=False
    )

    print("ETL COMPLETADO")


# =========================================
# EJECUCION
# =========================================

if __name__ == "__main__":

    ejecutar_pipeline()