import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow.models import DAG, Variable
import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user=Variable.get("user_redshift")
pwd= Variable.get("secret_pass_redshift")
client_id_Spotify= Variable.get("client_id_Spotify")
client_secret_Spotify= Variable.get("client_secret_Spotify")
def get_top_songs():

    client_id =client_id_Spotify
    client_secret =client_secret_Spotify
    client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    results = sp.search(q='year:2023', type='track', limit=50)
    data = {'Id': [],'Artista': [], 'Cancion': [],'Duracion_ms': [], 'Genero': [],'Album': [], 'Album_img': [], 'Total_canciones_album': [], 'Popularidad': [], 'fecha_lanzamiento': [],'fecha_modificacion': []}
    for track in results['tracks']['items']:
        id = track['id']
        artist_name = track['artists'][0]['name']
        artist_id = track['artists'][0]['id']
        track_name = track['name']
        duration_ms = track['duration_ms']
        track_id = track['id']
        album_group = track['album']['name']
        album_img = track['album']['images'][0]['url'] #imagen de album
        album_cont = track['album']['total_tracks']
        track_genre = sp.artist(artist_id)['genres']
        track_popularity = track['popularity']
        track_year = track['album']['release_date']
        #Quitar las comillas 
        track_name = track_name.replace("'", "")
        album_group = album_group.replace("'", "")
        #Separar el género por coma
        track_genre = ', '.join(track_genre)
        now = datetime.date.today().strftime('%Y-%m-%d')

        data['Id'].append(id)
        data['Artista'].append(artist_name)
        data['Cancion'].append(track_name)
        data['Duracion_ms'].append(duration_ms)
        data['Album'].append(album_group)
        data['Album_img'].append(album_img)
        data['Total_canciones_album'].append(album_cont)
        data['Genero'].append(track_genre)
        data['Popularidad'].append(track_popularity)
        data['fecha_lanzamiento'].append(track_year)
        data['fecha_modificacion'].append(now)


    df = pd.DataFrame(data)
    #Evitar que haya canciones duplicadas
    df.drop_duplicates(subset=['Artista', 'Cancion','Album'], keep='first', inplace=True)
    #Reemplazar valores nulos o vacios en el campo Género por Desconocido
    df['Genero'].fillna('Desconocido', inplace=True)
    df.loc[df['Genero'] == '', 'Genero'] = 'Desconocido'
    #Evitar que se cargue una canción con duración 0 ms
    df = df[df['Duracion_ms'] != 0]
    #Verificar que la fecha se muestre en formato fecha 
    df['fecha_lanzamiento'] = pd.to_datetime(df['fecha_lanzamiento'], format='%Y-%m-%d')
    df['fecha_lanzamiento'] = df['fecha_lanzamiento'].dt.strftime('%Y-%m-%d')
    df=df.to_dict()
    return(df)


def conectar_Redshift():
    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conectado a Redshift con éxito!")

    except Exception as e:
        print("No es posible conectar a Redshift")
        print(e)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS glo_tarcaya_coderhouse.canciones
            (
            id VARCHAR(50) primary key  
            ,artista VARCHAR(255)   
            ,cancion VARCHAR(255)  
            ,genero VARCHAR(300)   
            ,album VARCHAR(200)   
            ,total_canciones_album INTEGER  
            ,Popularidad INTEGER 
            ,fecha_lanzamiento date   
            ,duracion_ms INTEGER   
            ,album_img VARCHAR(300)
            ,fecha_modificacion date 
            )
        """)
        conn.commit()
    with conn.cursor() as cur:
        cur.execute("Truncate table canciones")
        count = cur.rowcount
        conn.close() 

def insert_data():
    conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
    data_dict = get_top_songs()
    df = pd.DataFrame(data_dict)
    #data = [(row['Id'], row['Artista'], row['Cancion'], row['Duracion_ms'], row['Genero'], row['Album'], row['Album_img'], row['Total_canciones_album'], row['Popularidad'], row['fecha_lanzamiento']) for _, row in df.iterrows()]
    print(df)
    with conn.cursor() as cur:
        try:
            execute_values(
                cur,
                '''
                    INSERT INTO canciones (Id, Artista, Cancion, Duracion_ms, Genero, Album, Album_img, Total_canciones_album,Popularidad,fecha_lanzamiento,fecha_modificacion)
                    VALUES %s
                    ''',
                    [tuple(row) for row in df.to_numpy()],
                    #data,
                    page_size=len(df)
                )
            conn.commit()
            conn.close()
        except Exception as e:
            print("No es posible insertar datos")
            print(e)
            
Pass_Email= Variable.get("secret_pass_gmail")
smtp_server = 'smtp.gmail.com'
smtp_port = 587
sender_email = 'glo.tarcaya@gmail.com'
password = Pass_Email


def send_email():
        try:
            subject = 'Carga de datos'
            body_text = 'Los datos fueron cargados a la base de datos exitosamente.'

            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = sender_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body_text, 'plain'))
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(sender_email, password)
                server.send_message(msg)
            print('El email fue enviado correctamente.')

        except Exception as exception:
            print(exception)
            print('El email no se pudo enviar.')