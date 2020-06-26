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

        if arrival_delay=='':
            arrival_delay=0
        if departure_delay=='':
            departure_delay=0
        departure_delay,arrival_delay=float(departure_delay),float(arrival_delay)

        yield airline,(departure_delay,arrival_delay)

    def reducer(self, key, values):
        num=0
        arrival_value=0
        departure_value=0
        for value in values:
            departure_value=departure_value+value[0]
            arrival_value=arrival_value+value[1]
            num=num+1

        yield key,(departure_value/num,arrival_value/num)



if __name__ == '__main__':
    MRFlights.run()
