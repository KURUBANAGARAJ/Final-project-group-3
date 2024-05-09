import pandas as pd
import boto3
from io import BytesIO

def lambda_handler(event, context):
    # Read Crimes data
    s3 = boto3.client('s3')
    crimes_obj = s3.get_object(Bucket='final-project-group3-data', Key='data-LZ2/chicago_crime_data.csv')
    crimes_df = pd.read_csv(crimes_obj['Body'])

    # Drop specified columns
    columns_to_drop = ['X Coordinate', 'Y Coordinate', 'Latitude', 'Longitude', 'Location','Domestic','Beat','District','Ward','Community Area','FBI Code','Year','Updated On']
    crimes_df = crimes_df.drop(columns=columns_to_drop)

    # Remove "100XX S" from the Block column
    crimes_df['Block'] = crimes_df['Block'].str.replace(r'^\d+XX\s\S+\s', '', regex=True)

    # Rename 'Block' column to 'Address'
    crimes_df = crimes_df.rename(columns={'Block': 'Address'})

    # Read Schools data
    schools_obj = s3.get_object(Bucket='final-project-group3-data', Key='data-LZ1/chicago_school_api_data.csv')
    schools_df = pd.read_csv(schools_obj['Body'])

    # Keep only specified columns in schools_df
    columns_to_keep = ['school_id', 'school_type', 'primary_category', 'is_high_school', 
                       'is_middle_school', 'is_elementary_school', 'is_pre_school', 
                       'address', 'city', 'state', 'zip', 'student_count_total', 
                       'student_count_low_income', 'student_count_special_ed', 
                       'student_count_english_learners', 'student_count_black', 
                       'student_count_hispanic', 'student_count_white', 'student_count_asian', 
                       'student_count_native_american', 'student_count_other_ethnicity', 
                       'student_count_asian_pacific_islander', 'student_count_multi', 
                       'student_count_hawaiian_pacific_islander', 
                       'student_count_ethnicity_not_available', 'college_enrollment_rate_mean', 
                       'graduation_rate_mean', 'overall_rating', 'rating_status', 
                       'rating_statement']
    schools_df = schools_df[columns_to_keep]

    # Remove "100XX S" from the address column
    schools_df['Address'] = schools_df['address'].str.replace(r'^\d+\s\w\s', '', regex=True)

    # Drop 'address' column
    schools_df.drop(columns='address', inplace=True)

    # Merge the dataframes based on 'Address' column
    merged_df = pd.merge(crimes_df, schools_df, on='Address', how='inner')

    # Write merged_df to S3
    csv_buffer = BytesIO()
    merged_df.to_csv(csv_buffer, index=False)
    s3.put_object(Body=csv_buffer.getvalue(), Bucket='final-project-group3-data', Key='data-LZ3/merged_data.csv')

    return {
        'statusCode': 200,
        'body': 'Merged data saved to S3 successfully!'
    }
