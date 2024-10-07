import pandas as pd
from DbConnector import DbConnector
from tabulate import tabulate
import os
from datetime import datetime
from tqdm import tqdm
import time



class ExampleProgram:


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


    def answer_one(self):
        print('Assignment 1')
        query = "SELECT COUNT(id) as unique_users FROM User"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

        query = "SELECT COUNT(id) as unique_activities FROM Activity"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

        query = "SELECT COUNT(id) as unique_trackpoints FROM TrackPoint"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
    
    def answer_two(self):
        query = """
            SELECT AVG(activity_count) AS avg_activities_per_user
            FROM (
                SELECT User.id, COUNT(Activity.id) AS activity_count
                FROM USER
                LEFT JOIN Activity ON User.id = Activity.user_id
                GROUP BY User.id
            ) AS user_activity_counts;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print('Assignment 2')
        print(tabulate(rows, headers=self.cursor.column_names))


    def answer_three(self):
        query = """
            SELECT user_id, COUNT(id) AS activity_count
            FROM Activity
            GROUP BY user_id
            ORDER BY activity_count DESC
            LIMIT 20;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print('Assignment 3')
        print(tabulate(rows, headers=self.cursor.column_names))


    def answer_four(self):
        query = """
            SELECT user_id
            FROM Activity
            WHERE transportation_mode = "taxi"
            GROUP BY user_id;
            
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print('Assignment 4')
        print(tabulate(rows, headers=self.cursor.column_names))


    def answer_five(self):
        query = """
            SELECT transportation_mode, COUNT(id) as activity_count
            FROM Activity
            WHERE transportation_mode is not NULL
            GROUP BY transportation_mode
            ORDER BY activity_count DESC;
            
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print('Assignment 5')
        print(tabulate(rows, headers=self.cursor.column_names))


    def answer_six(self):
        query = """
            SELECT 
                YEAR(start_date_time) as year, 
                COUNT(id) as activity_count, 
                SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS total_hours
            FROM Activity
            GROUP BY year
            ORDER BY year;
            
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print('Assignment 6')
        print(tabulate(rows, headers=self.cursor.column_names))

    def answer_seven(self):
        query = """

        SELECT SUM(distance) as total_distance FROM (
            SELECT 
                # Calculate haversine distance between identified trackpoints
                6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS(tp2.lat - tp1.lat) / 2), 2) + COS(RADIANS(tp1.lat)) * COS(RADIANS(tp2.lat)) * POWER(SIN(RADIANS(tp2.lon - tp1.lon) / 2), 2))) as distance
            FROM TrackPoint tp1

            # Join on activity to allow filtering by user id and transportation mode
            JOIN Activity ON Activity.id = tp1.activity_id
            
            # Retrieve cartesian product of all (successive) TrackPoints
            JOIN TrackPoint tp2 ON tp1.activity_id = tp2.activity_id 
                AND tp1.date_time < tp2.date_time
            
            # Filter user id
            WHERE Activity.user_id = '112'

            # Filter transportation mode
            AND Activity.transportation_mode = 'walk'

            # Filter year
            AND YEAR(Activity.start_date_time) = 2008

            # Filter only directly successive TrackPoints
            AND tp2.id = (
                SELECT id
                FROM TrackPoint 
                # Ensure trackpoint belongs to the same activity
                WHERE activity_id = tp1.activity_id 
                # Ensure trackpoint timestamp is greater than tp1
                AND date_time > tp1.date_time
                # Order timestamps to find timestamp directly after tp1
                ORDER BY date_time ASC
                LIMIT 1
                )
        ) as total_distance;
        
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print('Assignment 7')
        print(tabulate(rows, headers=self.cursor.column_names))

    def answer_eight(self):
        query = """
        SELECT 
            user_id,
            SUM(alt_gain) as total_altitude_gain
        FROM (
            SELECT
                user_id, 
                (GREATEST(tp2.altitude - tp1.altitude, 0) * 0.3048) as alt_gain
                
            FROM Activity
            
            LEFT JOIN TrackPoint as tp1 ON Activity.id = tp1.id
            LEFT JOIN TrackPoint as tp2 ON Activity.id = tp1.id AND tp1.date_time < tp2.date_time


            WHERE tp1.altitude != -777
            AND tp2.altitude != -777

            # Filter only directly successive TrackPoints
            AND tp2.id = (
                SELECT id
                FROM TrackPoint 
                # Ensure trackpoint belongs to the same activity
                WHERE activity_id = tp1.activity_id 
                # Ensure trackpoint timestamp is greater than tp1
                AND date_time > tp1.date_time
                # Order timestamps to find timestamp directly after tp1
                ORDER BY date_time ASC
                LIMIT 1
            )
        ) AS cartesian

        GROUP BY user_id
        ORDER BY total_altitude_gain DESC
        LIMIT 20;    
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print('Assignment 8')
        print(tabulate(rows, headers=self.cursor.column_names))


    def answer_nine(self):
        query = """
        SELECT 
            user_id,
            COUNT(activity_id) as illegals_count
        FROM (
            SELECT
                Activity.user_id as user_id, 
                Activity.id as activity_id,
                UNIX_TIMESTAMP(tp2.date_time) - UNIX_TIMESTAMP(tp1.date_time) as time_diff
                
            FROM Activity
            
            LEFT JOIN TrackPoint as tp1 ON Activity.id = tp1.id
            LEFT JOIN TrackPoint as tp2 ON Activity.id = tp1.id AND tp1.date_time < tp2.date_time

            # Filter only directly successive TrackPoints
            WHERE tp2.id = (
                SELECT id
                FROM TrackPoint 
                # Ensure trackpoint belongs to the same activity
                WHERE activity_id = tp1.activity_id 
                # Ensure trackpoint timestamp is greater than tp1
                AND date_time > tp1.date_time
                # Order timestamps to find timestamp directly after tp1
                ORDER BY date_time ASC
                LIMIT 1
            )
        ) AS cartesian
        WHERE time_diff > 60 * 5
        GROUP BY user_id
        ORDER BY illegals_count DESC;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print('Assignment 9')
        print(tabulate(rows, headers=self.cursor.column_names))

    def answer_ten(self):
        query = """
            SELECT
                DISTINCT(User.id)
            FROM User
            JOIN Activity ON User.id = Activity.user_id
            JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
            WHERE ROUND(TrackPoint.lat, 3) = 39.916 AND ROUND(TrackPoint.lon, 3) = 116.397;

        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print('Assignment 10')
        print(tabulate(rows, headers=self.cursor.column_names))

    def answer_eleven(self):
        query = """
            SELECT
                User.id,
                Activity.transportation_mode,
                COUNT(Activity.transportation_mode)
                
            FROM User
            LEFT JOIN Activity ON User.id = Activity.user_id
            WHERE Activity.transportation_mode IS NOT NULL
            GROUP BY User.id, Activity.transportation_mode

        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        
        maxes = {}
        for user_id, transportation_mode, count in rows:
            if not user_id in maxes:
                maxes[user_id] = (transportation_mode, count)
                continue
            if count > maxes[user_id][1]:
                maxes[user_id] = (transportation_mode, count)
        
        maxes_list = []
        for key, val in maxes.items():
            maxes_list.append((key, val[0]))
        print('Assignment 11')
        print(tabulate(maxes_list))
    
    
    def answer(self):
        self.answer_one()
        self.answer_two()
        self.answer_three()
        self.answer_four()
        self.answer_five()
        self.answer_six()
        self.answer_seven()
        self.answer_eight()
        self.answer_nine()
        self.answer_ten()
        self.answer_eleven()


def main():
    program = None
    program = ExampleProgram()
    #program.create_tables()
    #program.insert_users()
    #program.insert_activities_trackpoints()
    
    
    now = time.time()
    program.answer()
    print('Elapsed seconds: ', time.time() - now)

    if program:
        program.connection.close_connection()


if __name__ == '__main__':
    main()




