from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class MRFlights(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        (year, month, day, day_of_week, airline, flight_number, tail_number, origin_airport,
         destination_airport, scheduled_departure, departure_time, departure_delay, taxi_out,
         wheels_off, scheduled_time, elapsed_time, air_time, distance, wheels_on, taxi_in,
         scheduled_arrival, arrival_time, arrival_delay, diverted, cancelled, cancellation_reason,
         air_system_delay, security_delay, airline_delay, late_aircraft_delay, weather_delay) = line.split(',')

        yield airline,cancelled

    def reducer(self, key, values):
        num=0
        can=0

        for value in values:
            if int(value)!=0:
                can=can+1
            num=num+1

        yield key,can/num


if __name__ == '__main__':
    MRFlights.run()
