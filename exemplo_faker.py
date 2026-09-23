import csv
from faker import Faker

fake = Faker('pt_BR')

with open('usuarios.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['nome', 'email', 'nascimento'])
    writer.writeheader()
    
    for _ in range(100):
        writer.writerow({
            'nome': fake.name(),
            'email': fake.email(),
            'nascimento': fake.date_of_birth()
        })
