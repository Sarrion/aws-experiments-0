def load(path, bucket_name = 'bme-bucket'):
    import io
    import pickle
    import boto3
        
    s3_client = boto3.client('s3')
    array = io.BytesIO()
    s3_client.download_fileobj(bucket_name, path, array)
    array.seek(0)
    return pickle.load(array)


def df_to_csv_on_s3(dataframe, filename, DESTINATION = 'bme-bucket'):
    import boto3
    from io import StringIO
    
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)
    s3_resource = boto3.resource("s3") # Create S3 object
    return s3_resource.Object(DESTINATION, filename).put(Body=csv_buffer.getvalue()) # Write buffer to S3 object
    
    
def get_merged_perfiles_fondos():
    import pandas as pd
    import s3fs
    
    s3 = s3fs.S3FileSystem()
    file = 's3://bme-bucket/engineered_data/merged_perfiles_fondos.csv'
    if s3.exists(file):
        result = pd.read_csv(file)
    else:
        print('Creating dataframe...')
        perfiles     = pd.read_csv('s3://bme-bucket/perfiles_inversores.csv', nrows = 79899)
        fondos       = pd.read_csv('s3://bme-bucket/asignacion_fondos.csv', nrows = 79899)
        tabla_fondos = pd.read_csv('s3://bme-bucket/tabla_fondos.csv', index_col=0)
        
#         perfiles_Qs = perfiles.columns
        perfiles.columns = ['Q{}'.format(i) for i in range(perfiles.shape[1])]
    
        per_fon = pd.concat([perfiles, fondos], axis = 1, sort = False)
        
        for i in tabla_fondos.columns[2:-1]:
            tabla_fondos[i] = tabla_fondos[i].apply(lambda x: float(x[:-1]))
            
        for i in range(1, fondos.shape[1] + 1):
            if i == 1:
                result = per_fon
        
            result = pd.merge(df, tabla_fondos, how = 'left', left_on = 'Fondo {}'.format(i), right_on = 'Nombre fondo')\
            .rename(columns = {j:j+'_'+str(i) for j in tabla_fondos.columns})\
            .drop(['Fondo {}'.format(i),
                   'Nombre fondo_{}'.format(i),
                   'Categoria_{}'.format(i)], axis = 'columns')
    return result