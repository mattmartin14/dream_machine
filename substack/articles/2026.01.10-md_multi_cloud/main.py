import scripts
def main():
    scripts.generate_orders()
    scripts.generate_insights()
    scripts.push_to_clouds()
   
if __name__ == "__main__":
    main()
