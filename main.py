from API import Dam_API



if __name__ == "__main__":
    dam_api = Dam_API()
    # dam_api.Get_Dam_Code() # Dam 코드 조회후 저장 -> 최초 1 회만 실행
    
    dam_api.Get_Dam_Data()