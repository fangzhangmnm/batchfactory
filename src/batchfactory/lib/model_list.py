client_desc = {
    'openai':{
        'api_key_environ':'OPENAI_API_KEY',
        'base_url':None,
    },
    'lambda':{
        'api_key_environ': 'LAMBDA_API_KEY',
        'base_url': 'https://api.lambdalabs.com/v1',
    },
    'deepseek':{
        'api_key_environ': 'DEEPSEEK_API_KEY',
        'base_url': 'https://api.deepseek.com/v1',
    }
}

model_desc = {
    'o3-mini-2025-01-31@openai':{
        'price_per_input_token_M':0.55,
        'price_per_output_token_M':2.20
    },
    'gpt-4o-2024-08-06@openai':{
        'price_per_input_token_M':1.25,
        'price_per_output_token_M':5
    },
    'gpt-4o-2024-11-20@openai':{
        'price_per_input_token_M':1.25,
        'price_per_output_token_M':5
    },
    'gpt-4o-mini-2024-07-18@openai':{
        'price_per_input_token_M':0.075,
        'price_per_output_token_M':0.3
    },
    'gpt-4o-mini@openai':{
        'price_per_input_token_M':0.075,
        'price_per_output_token_M':0.3
    },
    'deepseek-r1-671b@lambda':{
        'price_per_input_token_M':0.54,
        'price_per_output_token_M':2.18
    },
    'deepseek-v3-0324@lambda':{
        'price_per_input_token_M':0.34,
        'price_per_output_token_M':0.88
    },
    'deepseek-llama3.3-70b@lambda':{
        'price_per_input_token_M':0.20,
        'price_per_output_token_M':0.60
    },
    'llama3.1-405b-instruct-fp8@lambda':{
        'price_per_input_token_M':0.80,
        'price_per_output_token_M':0.80
    },
    'llama3.1-70b-instruct-fp8@lambda':{
        'price_per_input_token_M':0.12,
        'price_per_output_token_M':0.30
    },
    'llama3.1-8b-instruct@lambda':{
        'price_per_input_token_M':0.025,
        'price_per_output_token_M':0.04
    },
    'llama-4-maverick-17b-128e-instruct-fp8@lambda':{
        'price_per_input_token_M':0.18,
        'price_per_output_token_M':0.60
    },
    'hermes-3-llama-3.1-405b-fp8@lambda':{
        'price_per_input_token_M':0.80,
        'price_per_output_token_M':0.80
    },
}