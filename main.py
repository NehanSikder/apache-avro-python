
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


if __name__ == "__main__":
	print("testing avro")
	schema = avro.schema.parse(open("user.avsc", "rb").read())
	writer = DataFileWriter(open("users.avro","wb"), DatumWriter(), schema)
	writer.append({"name": "Al", "favorite_number": 256})
	writer.append({"name": "Bob", "favorite_number": 7, "favorite_color": "red"})
	writer.close()

	reader = DataFileReader(open("users.avro","rb"),DatumReader())
	for user in reader:
		print(user)
	reader.close()




