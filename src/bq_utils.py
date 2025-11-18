"""
Utilitaires pour le chargement de données depuis GCS vers BigQuery.

Ce module fournit des fonctions pour charger différents formats de fichiers
depuis Google Cloud Storage vers BigQuery.
"""

from typing import List, Optional

from google.cloud import bigquery, storage


def load_parquet_from_gcs(
    gcs_path: str,
    table_name: str,
    bq_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    bucket_name: str,
    schema: Optional[List[bigquery.SchemaField]] = None,
    primary_key: Optional[str] = None,
    write_disposition: str = "WRITE_TRUNCATE"
) -> str:
    """
    Charge un fichier Parquet depuis GCS vers BigQuery.
    
    Args:
        gcs_path: Chemin du fichier dans GCS (sans gs://bucket/)
        table_name: Nom de la table BigQuery
        bq_client: Client BigQuery
        project_id: ID du projet GCP
        dataset_id: ID du dataset BigQuery
        bucket_name: Nom du bucket GCS
        schema: Schéma BigQuery optionnel (si None, autodetect activé)
        primary_key: Nom de la clé primaire (pour affichage uniquement)
        write_disposition: Mode d'écriture ("WRITE_TRUNCATE" ou "WRITE_APPEND")
    
    Returns:
        str: ID complet de la table créée (project.dataset.table)
    
    Raises:
        google.cloud.exceptions.GoogleCloudError: Si le chargement échoue
    """
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    gcs_uri = f"gs://{bucket_name}/{gcs_path}"
    
    print(f"\n[...] - Chargement de {gcs_uri} vers {table_id}...")
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
    )
    
    if schema:
        job_config.schema = schema
    else:
        job_config.autodetect = True
    
    load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
    
    table = bq_client.get_table(table_id)
    print(f"[OK] - {table.num_rows} lignes chargées dans {table_id}")
    print(f"[OK] - Taille: {table.num_bytes / (1024*1024):.2f} MB")
    if primary_key:
        print(f"[OK] - Clé primaire: {primary_key}")
    
    return table_id


def load_csv_from_gcs(
    gcs_path: str,
    table_name: str,
    bq_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    bucket_name: str,
    schema: Optional[List[bigquery.SchemaField]] = None,
    skip_leading_rows: int = 1,
    write_disposition: str = "WRITE_TRUNCATE",
    encoding: Optional[str] = None,
    sep: Optional[str] = None,
    date_format: Optional[str] = None,
    datetime_format: Optional[str] = None,
    storage_client: Optional[storage.Client] = None
) -> str:
    """
    Charge un fichier CSV depuis GCS vers BigQuery.
    
    Args:
        gcs_path: Chemin du fichier dans GCS (sans gs://bucket/)
        table_name: Nom de la table BigQuery
        bq_client: Client BigQuery
        project_id: ID du projet GCP
        dataset_id: ID du dataset BigQuery
        bucket_name: Nom du bucket GCS
        schema: Schéma BigQuery optionnel (si None, autodetect activé)
        skip_leading_rows: Nombre de lignes d'en-tête à ignorer (défaut: 1)
        write_disposition: Mode d'écriture ("WRITE_TRUNCATE" ou "WRITE_APPEND")
        encoding: Encodage du fichier ("UTF8" ou "ISO_8859_1"). Si "utf-16le", le fichier sera converti en UTF-8
        sep: Séparateur de champ (défaut: virgule). Peut être "\t" pour tabulation, ";" pour point-virgule, etc.
        date_format: Format de date utilisé pour parser les valeurs DATE (ex: "%Y-%m-%d", "%d/%m/%Y")
        datetime_format: Format de date/heure utilisé pour parser les valeurs DATETIME (ex: "%Y-%m-%d %H:%M:%S")
        storage_client: Client GCS (requis si encoding="utf-16le" pour la conversion)
    
    Returns:
        str: ID complet de la table créée (project.dataset.table)
    
    Raises:
        google.cloud.exceptions.GoogleCloudError: Si le chargement échoue
        ValueError: Si encoding="utf-16le" mais storage_client n'est pas fourni
    """
    table_id = f"{project_id}.{dataset_id}.{table_name}"
    
    # Si l'encodage est UTF-16LE, BigQuery ne peut pas le lire directement
    # Il faut convertir le fichier en UTF-8 avant le chargement
    temp_blob = None
    temp_gcs_path = None
    needs_utf16_conversion = encoding and encoding.lower() in ["utf-16le", "utf-16-le"]
    
    if needs_utf16_conversion:
        if not storage_client:
            raise ValueError("storage_client est requis pour convertir les fichiers UTF-16LE en UTF-8")
        
        print(f"\n[...] - Conversion UTF-16LE -> UTF-8 pour {gcs_path}...")
        
        # Télécharger le fichier depuis GCS
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(gcs_path)
        content = blob.download_as_bytes()
        
        # Convertir UTF-16LE -> UTF-8
        text_content = content.decode('utf-16le')
        utf8_content = text_content.encode('utf-8')
        
        # Uploader la version UTF-8 dans un emplacement temporaire
        temp_gcs_path = f"{gcs_path}.utf8"
        temp_blob = bucket.blob(temp_gcs_path)
        temp_blob.upload_from_string(utf8_content, content_type='text/plain')
        
        print(f"[OK] - Fichier converti et uploadé vers {temp_gcs_path}")
        
        # Utiliser le fichier temporaire pour le chargement
        gcs_uri = f"gs://{bucket_name}/{temp_gcs_path}"
        # Forcer l'encodage à UTF-8 pour BigQuery
        encoding = "utf-8"
    else:
        gcs_uri = f"gs://{bucket_name}/{gcs_path}"
    
    print(f"\n[...] - Chargement de {gcs_uri} vers {table_id}...")
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
        skip_leading_rows=skip_leading_rows,
    )
    
    # Configurer l'encodage
    if encoding:
        if encoding.lower() in ["utf-8", "utf8"]:
            job_config.encoding = bigquery.Encoding.UTF_8
        elif encoding.lower() in ["latin-1", "iso-8859-1"]:
            job_config.encoding = bigquery.Encoding.ISO_8859_1
        else:
            # Par défaut UTF_8 si encodage non reconnu
            job_config.encoding = bigquery.Encoding.UTF_8
    
    # Configurer le séparateur
    if sep:
        # Convertir "\t" en tabulation réelle
        if sep == "\\t":
            sep = "\t"
        job_config.field_delimiter = sep
    
    # Configurer le format de date
    if date_format:
        job_config.date_format = date_format
    
    # Configurer le format de datetime
    if datetime_format:
        job_config.datetime_format = datetime_format
    
    if schema:
        job_config.schema = schema
    else:
        job_config.autodetect = True
    
    load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
    
    table = bq_client.get_table(table_id)
    print(f"[OK] - {table.num_rows} lignes chargées dans {table_id}")
    print(f"[OK] - Taille: {table.num_bytes / (1024*1024):.2f} MB")
    
    # Nettoyer le fichier temporaire si créé
    if temp_blob and temp_gcs_path:
        try:
            temp_blob.delete()
            print(f"[OK] - Fichier temporaire {temp_gcs_path} supprimé")
        except Exception as e:
            print(f"[WARN] - Impossible de supprimer le fichier temporaire {temp_gcs_path}: {e}")
    
    return table_id