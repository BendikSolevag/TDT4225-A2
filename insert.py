import pandas as pd
from DbConnector import DbConnector
import os
from datetime import datetime
from tqdm import tqdm




class InsertProgram:


    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.basepath = "/Users/bendiksolevag/Documents/NTNU/TDT4225/Exercise2"

    def create_tables(self):
        query = """
                CREATE TABLE IF NOT EXISTS User (
                   id VARCHAR(3) NOT NULL,
                   has_labels BOOLEAN DEFAULT false,
                   PRIMARY KEY (id)
                )
                """
        self.cursor.execute(query)
        query = """
                CREATE TABLE IF NOT EXISTS Activity (
                    id INT NOT NULL AUTO_INCREMENT,
                    user_id VARCHAR(3) NOT NULL,
                    transportation_mode VARCHAR(15),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    PRIMARY KEY (id),
                    FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
                )
                """
        self.cursor.execute(query)
        query = """
                CREATE TABLE IF NOT EXISTS TrackPoint (
                    id INT NOT NULL AUTO_INCREMENT,
                    activity_id INT NOT NULL,
                    lat DOUBLE,
                    lon DOUBLE,
                    altitude INT,
                    date_time DATETIME,
                    PRIMARY KEY (id),
                    FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE
                )
                """
        self.cursor.execute(query)
        self.db_connection.commit()
    
    def insert_users(self):
        # Create list of users who have documented activity labels
        user_ids_with_labels = []
        user_ids_with_labels_file = open(f"{self.basepath}/dataset/labeled_ids.txt", "r")
        for user_id in user_ids_with_labels_file:
            user_ids_with_labels.append(user_id.replace('\n', ''))

        query = "INSERT INTO User (id, has_labels) VALUES "
        i = 0
        for user_id in os.listdir(f"{self.basepath}/dataset/data"):
            if i != 0:
                query += ','
            has_label = '1' if user_id in user_ids_with_labels else '0'
            query += f'(\'{user_id}\', \'{has_label}\')'
            i += 1
        
        self.cursor.execute(query)
        self.db_connection.commit()


    def insert_activities_trackpoints(self):
        query = "SELECT id, has_labels FROM User"
        self.cursor.execute(query)
        db_users = self.cursor.fetchall()

        for i in tqdm(range(len(db_users))):
            user_id, has_labels = db_users[i]
            activity_filenames = os.listdir(self.basepath + f'/dataset/data/{user_id}/Trajectory')

            for activity_filename in activity_filenames:
                f = open(f'{self.basepath}/dataset/data/{user_id}/Trajectory/{activity_filename}', 'r')
                
                trackpoints = []
                activity_id = 0
                for i, line in enumerate(f):
                    # Ignore 6 garbage lines at start of each file
                    if i < 6:
                        continue
                    dets = line.replace('\n', '').split(',')
                    lat = dets[0]
                    lon = dets[1]
                    altitude = dets[3]
                    if altitude == -777:
                        altitude = None
                    date = dets[5]
                    time = dets[6]

                    dt = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M:%S")
                    
                    # INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_time) VALUES:
                    trackpoints.append((activity_id, lat, lon, altitude, dt.isoformat()))


                # If the activity is of greater length than 2500 trackpoints, ignore it
                if len(trackpoints) > 2500:
                    continue

                activity_user_id = user_id
                activity_transportation_mode = None
                activity_start_date_time = trackpoints[0][-1]
                activity_end_date_time = trackpoints[-1][-1]

                if has_labels:
                    # Read data to dataframe
                    df = pd.read_csv(f'{self.basepath}/dataset/data/{user_id}/labels.txt', sep='\t')
                    df['Start Time'] = pd.to_datetime(df['Start Time'])
                    df['End Time'] = pd.to_datetime(df['End Time'])
                    s = pd.Timestamp(activity_start_date_time)
                    e = pd.Timestamp(activity_end_date_time)
                    # Keep only rows overlapping with the activity time period
                    df = df[df['Start Time'] < e]
                    df = df[ df['End Time'] > s]

                    max_overlap = None
                    max_overlap_label = None
                    counter = 0
                    for index, row in df.iterrows():
                        counter += 1
                        start = max(s, row['Start Time'])
                        end = min(e, row['End Time'])
                        overlap = end - start

                        if counter == 1:
                            max_overlap = overlap
                            max_overlap_label = row['Transportation Mode']
                            continue
                        
                        if overlap > max_overlap:
                            max_overlap = overlap
                            max_overlap_label = row['Transportation Mode']

                    activity_transportation_mode = max_overlap_label
                    

                query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"
                self.cursor.execute(query, (activity_user_id, activity_transportation_mode, activity_start_date_time, activity_end_date_time))

                query = "SELECT id FROM Activity WHERE user_id = %s AND start_date_time = %s AND end_date_time = %s"
                self.cursor.execute(query, (activity_user_id, activity_start_date_time, activity_end_date_time))
                inserted_activity = self.cursor.fetchall()
                if len(inserted_activity) > 1:
                    raise ValueError('Identified more than one activity for given user and start/end date combination')
                activity_id = inserted_activity[0][0]


                for i in range(len(trackpoints)):
                    tp = trackpoints[i]
                    trackpoints[i] = (activity_id, tp[1], tp[2], tp[3], tp[4])
            
                query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_time) VALUES (%s, %s, %s, %s, %s)"
                self.cursor.executemany(query, trackpoints)
                self.db_connection.commit()


def main():
    program = None
    program = InsertProgram()
    program.create_tables()
    program.insert_users()
    program.insert_activities_trackpoints()
    if program:
        program.connection.close_connection()


if __name__ == '__main__':
    main()
