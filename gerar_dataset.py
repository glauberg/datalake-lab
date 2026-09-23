import csv
import random

from faker import Faker

fake = Faker('pt_BR')                                                                                           
Faker.seed(42)                                                                                                  
                                                                                                                    
# Gerar arquivo de vendas simulado                                                                              
with open('data/raw/vendas_detalhadas.csv', mode='w', newline='', encoding='utf-8') as f:                       
    writer = csv.writer(f)                                                                                      
    writer.writerow(["id_venda", "cliente_nome", "cliente_email", "estado", "produto", "categoria", "quantidade",
"preco_unitario", "data_venda"])                                                                                  
                                                                                                                
    produtos = [                                                                                                
        ("Notebook Gamer", "Eletrônicos", 4500.00),                                                             
        ("Mouse sem Fio", "Acessórios", 120.00),                                                                
        ("Teclado Mecânico", "Acessórios", 350.00),                                                             
        ("Monitor 27pol", "Eletrônicos", 1300.00),                                                              
        ("Cadeira Ergonômica", "Móveis", 950.00)                                                                
    ]                                                                                                           
                                                                                                                
    for i in range(1, 1001):                                                                                    
        prod, cat, preco = random.choice(produtos)                                                              
        qtd = random.randint(1, 5)                                                                              
        # introduzir eventuais duplicatas ou pequenas variações propositais para limpeza                        
        writer.writerow([                                                                                       
            i,                                                                                                  
            fake.name(),                                                                                        
            fake.email(),                                                                                       
            fake.state_abbr(),                                                                                  
            prod,                                                                                               
            cat,                                                                                                
            qtd,                                                                                                
            preco,                                                                                              
            fake.date_between(start_date='-1y', end_date='today').isoformat()                                   
        ])                                                                                                      
                                                                                                                
print("Dataset gerado com sucesso em data/raw/vendas_detalhadas.csv!")