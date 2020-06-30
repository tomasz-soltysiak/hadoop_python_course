from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRFlights(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer)
        ]

    def configure_args(self):
        super(MRFlights,self).configure_args()
        self.add_file_arg('--airlines',help='Path to the airlines.csv')

    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport,
         destination_airport, scheduled_departure, departure_time, departure_delay, taxi_out,
         wheels_off, scheduled_time, elapsed_time, air_time, distance, wheels_on, taxi_in,
         scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, cancellation_reason,
         air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay) = line.split(',')

        yield airline,cancelled

    def reducer_init(self):
        self.airlines_name = {}

        with open('airlines.csv', 'r') as file:
            for line in file:
                code, full_name = line.split(',')
                full_name = full_name[:-1]
                self.airlines_name[code] = full_name

    def reducer(self, key, values):
        num=0
        can=0

        for value in values:
            if int(value)!=0:
                can=can+1
            num=num+1

        yield self.airlines_name[key],round(can/num,4)


if __name__ == '__main__':
    MRFlights.run()
