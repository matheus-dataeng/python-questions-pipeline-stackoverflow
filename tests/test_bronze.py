from src.bronze.extract import extract
import pytest

def test_api_retorno(mocker):
    
    test_response = {
        "items": [
            {
                "tags": "python",
                "view_count": 10, 
                "answer_count": 1
            },
            
            {
                "tags" : "pandas",
                "carro" : "ford",
                "sabao" : "omo"
            }
        ]
    }
    
    mock_get = mocker.patch("requests.get")
    mock_get.return_value.json.return_value = test_response
    mock_get.return_value.status_code = 200
    
    resultado = extract()
    
    assert not resultado.empty
    assert "tags" in resultado.columns
    assert len(resultado) == 2
    
    