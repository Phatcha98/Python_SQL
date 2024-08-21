from apps.smt.models.model import SmtZprnProgramLogCleaningcondition
from apps.smt.models.model import SmtZprnProgramLogPrintCondition
from apps.smt.models.model import SmtZprnProgramLogPrintposition
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import psycopg2
import pandas as pd
import glob
import os
import queue
import schedule
import shutil
import sys

# Config
# INPUT_PATH = "/home/mnt/10_17_72_74/g/SMT/Print_SMT"
INPUT_PATH = "/home/mnt/10_17_72_74/g/SMT/Auto_Collect_Data_Print"

class Command(CustomBaseCommand):

    def save_condition_sp(self, condition_sp):
        tuple_insert = [tuple(x) for x in condition_sp.itertuples(index=False, name=None)]      
        db_in = connections["10.17.66.121.iot.smt"].settings_dict
        with  psycopg2.connect(user=db_in["USER"], password=db_in["PASSWORD"], host=db_in["HOST"], port=db_in["PORT"], dbname=db_in["NAME"], options=db_in['OPTIONS']['options']) as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                INSERT INTO smt.smt_print_program_log_printcondition
                (create_at, update_at, f_sq_speed, r_sq_speed, sq_press_f, sq_press_r, sq_length, prt_stroke_f, prt_stroke_r, 
                                clearance, lift_dwn_speed, lift_dwn_stroke, line_machine, program_name)
                VALUES (now(), now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, tuple_insert)
            conn.commit()

    def save_cleaning_sp(self, cleaning_sp):                                                                                                                                                                        
        tuple_insert = [tuple(x) for x in cleaning_sp.itertuples(index=False, name=None)]      
        db_in = connections["10.17.66.121.iot.smt"].settings_dict
        with  psycopg2.connect(user=db_in["USER"], password=db_in["PASSWORD"], host=db_in["HOST"], port=db_in["PORT"], dbname=db_in["NAME"], options=db_in['OPTIONS']['options']) as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                INSERT INTO smt.smt_print_program_log_cleaningcondition
                (create_at, update_at, cln_mode_1_flg, interval_mode_1, cln_mode_2_flg, interval_mode_2, 
                    auto_flg, auto_time, fwd_speed_1_st, bwd_speed_1_st, fwd_speed_2_nd, bwd_speed_2_nd, 
                    wet_flg_1_st, wet_flg_2_nd, fwd_vac_flg_1_st, bwd_vac_flg_1_st, fwd_vac_flg_2_nd, 
                    bwd_vac_flg_2_nd, line_machine, program_name, bwd_wet_flg_1_st, bwd_wet_flg_2_st, 
                    fwd_speed, bwd_speed, wet_flg, bwd_wet_flg, fwd_vac_flg, bwd_vac_flg, fwd_move_flg, 
                    bwd_move_flg, fwd_move_flg_1_st, bwd_move_flg_1_st, fwd_move_flg_2_st, bwd_move_flg_2_st)
                VALUES(now(), now(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, tuple_insert)
            conn.commit()

    def save_position_sp(self, position_sp):
        tuple_insert = [tuple(x) for x in position_sp.itertuples(index=False, name=None)]      
        db_in = connections["10.17.66.121.iot.smt"].settings_dict
        with  psycopg2.connect(user=db_in["USER"], password=db_in["PASSWORD"], host=db_in["HOST"], port=db_in["PORT"], dbname=db_in["NAME"], options=db_in['OPTIONS']['options']) as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                INSERT INTO smt.smt_print_program_log_printposition
                (create_at, update_at, x, y, angle, line_machine, program_name)
                VALUES (now(), now(), %s, %s, %s, %s, %s);
            """, tuple_insert)
            conn.commit()

    def condition_off_on(self, data):
        if data == '0':
            result = "OFF"
        elif data == '1':
            result = "ON"
        else:
            result = data
        return result
    
    def move_to_backup(self, file):
        BACKUP_PATH = os.path.join(os.path.dirname(file), "BACKUP")
        if not os.path.exists(BACKUP_PATH):
            os.makedirs(BACKUP_PATH)
        if os.path.exists(os.path.join(BACKUP_PATH, os.path.basename(file))):
            os.remove(os.path.join(BACKUP_PATH, os.path.basename(file)))
        shutil.move(file, BACKUP_PATH)
    
    def move_to_error(self, file):
        ERROR_PATH = os.path.join(os.path.dirname(file), "ERROR")
        if not os.path.exists(ERROR_PATH):
            os.makedirs(ERROR_PATH)
        if os.path.exists(os.path.join(ERROR_PATH, os.path.basename(file))):
            os.remove(os.path.join(ERROR_PATH, os.path.basename(file)))
        shutil.move(file, ERROR_PATH)

    @logger.catch
    def run(self):
        
        logger.log("START", None)
        line_folder = glob.glob(f"{INPUT_PATH}/Line*")
        
        for line in line_folder:
            machine_folder = glob.glob(f"{line}/*")
            for machine in machine_folder:
                file_list = glob.glob(f"{machine}/SET/*-R*")
                for file in file_list:
                    try:
                        if os.path.basename(file) == "BACKUP" or os.path.basename(file) == "ERROR":
                            continue
                        # elif not (os.path.basename(file).split('-')[-1])[0] == "R":
                        #     continue
                        # elif (os.path.exists(os.path.join(os.path.dirname(file), "BACKUP", os.path.basename(file))) or
                        #       os.path.exists(os.path.join(os.path.dirname(file), "BACKUP", "BACKUP_EWK_Calling", os.path.basename(file)))):
                        #     print(f"{file} --- duplicate file in backup")
                        #     continue
                        else:
                            file_read = open(file, 'r')
                            line = file_read.readlines()
                            
                            # Print Condition
                            position_print_condition_sp = line.index("[PrintConditionSP]\n")
                            condition_sp = pd.DataFrame({ 
                                'f_sq_speed': [float(line[position_print_condition_sp + 3].split('=')[1].strip())],
                                'r_sq_speed': [float(line[position_print_condition_sp + 4].split('=')[1].strip())],
                                'sq_press_f': [float(line[position_print_condition_sp + 7].split('=')[1].strip())],
                                'sq_press_r': [float(line[position_print_condition_sp + 8].split('=')[1].strip())],
                                'sq_length': [float(line[position_print_condition_sp + 9].split('=')[1].strip())],
                                'prt_stroke_f': [float(line[position_print_condition_sp + 12].split('=')[1].strip())],
                                'prt_stroke_r': [float(line[position_print_condition_sp + 13].split('=')[1].strip())],
                                'clearance': [float(line[position_print_condition_sp + 14].split('=')[1].strip())],
                                'lift_dwn_speed': [float(line[position_print_condition_sp + 15].split('=')[1].strip())],
                                'lift_dwn_stroke': [float(line[position_print_condition_sp + 16].split('=')[1].strip())],
                                'line_machine': [file.split('/')[-3]],
                                'program_name': [os.path.basename(file)]
                            })
                            self.save_condition_sp(condition_sp)

                            # Cleaning Condition
                            position_cleaning_sp = line.index("[CleaningSP]\n")
                            cleaning_sp = pd.DataFrame({ 
                                'cln_mode_1_flg' : [self.condition_off_on(line[position_cleaning_sp + 1].split('=')[1].split('\n')[0])],
                                'interval_mode_1' : [line[position_cleaning_sp + 2].split('=')[1].split('\n')[0]],
                                'cln_mode_2_flg' : [self.condition_off_on(line[position_cleaning_sp + 3].split('=')[1].split('\n')[0])],
                                'interval_mode_2' : [line[position_cleaning_sp + 4].split('=')[1].split('\n')[0]],
                                'auto_flg' : [self.condition_off_on(line[position_cleaning_sp + 5].split('=')[1].split('\n')[0])],
                                'auto_time' : [line[position_cleaning_sp + 6].split('=')[1].split('\n')[0]],
                                'fwd_speed_1_st' : [line[position_cleaning_sp + 7].split('=')[1].split('\n')[0]],
                                'bwd_speed_1_st' : [line[position_cleaning_sp + 8].split('=')[1].split('\n')[0]],
                                'fwd_speed_2_nd' : [line[position_cleaning_sp + 9].split('=')[1].split('\n')[0]],
                                'bwd_speed_2_nd' : [line[position_cleaning_sp + 10].split('=')[1].split('\n')[0]],
                                'wet_flg_1_st' : [self.condition_off_on(line[position_cleaning_sp + 11].split('=')[1].split('\n')[0])],
                                'wet_flg_2_nd' : [self.condition_off_on(line[position_cleaning_sp + 12].split('=')[1].split('\n')[0])],
                                'fwd_vac_flg_1_st': [self.condition_off_on(line[position_cleaning_sp + 13].split('=')[1].split('\n')[0])],
                                'bwd_vac_flg_1_st' : [self.condition_off_on(line[position_cleaning_sp + 14].split('=')[1].split('\n')[0])],
                                'fwd_vac_flg_2_nd' : [self.condition_off_on(line[position_cleaning_sp + 15].split('=')[1].split('\n')[0])],
                                'bwd_vac_flg_2_nd' : [self.condition_off_on(line[position_cleaning_sp + 16].split('=')[1].split('\n')[0])],
                                'line_machine' : [ file.split('/')[-3]],
                                'program_name' : [os.path.basename(file)],
                                'bwd_wet_flg_1_st' : [self.condition_off_on(float(line[position_cleaning_sp + 17].split('=')[1].split('\n')[0]))],
                                'bwd_wet_flg_2_st' : [self.condition_off_on(float(line[position_cleaning_sp + 18].split('=')[1].split('\n')[0]))],
                                'fwd_speed' : [float(line[position_cleaning_sp + 19].split('=')[1].split('\n')[0])],
                                'bwd_speed' : [float(line[position_cleaning_sp + 20].split('=')[1].split('\n')[0])],
                                'wet_flg' : [self.condition_off_on(float(line[position_cleaning_sp + 21].split('=')[1].split('\n')[0]))],
                                'bwd_wet_flg' : [self.condition_off_on(float(line[position_cleaning_sp + 22].split('=')[1].split('\n')[0]))],
                                'fwd_vac_flg' : [self.condition_off_on(float(line[position_cleaning_sp + 23].split('=')[1].split('\n')[0]))],
                                'bwd_vac_flg' : [self.condition_off_on(float(line[position_cleaning_sp + 24].split('=')[1].split('\n')[0]))],
                                'fwd_move_flg' : [self.condition_off_on(float(line[position_cleaning_sp + 25].split('=')[1].split('\n')[0]))],
                                'bwd_move_flg' : [self.condition_off_on(float(line[position_cleaning_sp + 26].split('=')[1].split('\n')[0]))],
                                'fwd_move_flg_1_st' : [self.condition_off_on(float(line[position_cleaning_sp + 27].split('=')[1].split('\n')[0]))],
                                'bwd_move_flg_1_st' : [self.condition_off_on(float(line[position_cleaning_sp + 28].split('=')[1].split('\n')[0]))],
                                'fwd_move_flg_2_st' : [self.condition_off_on(float(line[position_cleaning_sp + 29].split('=')[1].split('\n')[0]))],
                                'bwd_move_flg_2_st' : [self.condition_off_on(float(line[position_cleaning_sp + 30].split('=')[1].split('\n')[0]))]
                            })
                            self.save_cleaning_sp(cleaning_sp)

                            # Print Position
                            position_print_position_sp = line.index("[PrintPositionSP]\n")
                            position_sp = pd.DataFrame({ 
                                'x' : [float(line[position_print_position_sp + 1].split('=')[1].split('\n')[0])],
                                'y' : [float(line[position_print_position_sp + 2].split('=')[1].split('\n')[0])],
                                'angle' : [float(line[position_print_position_sp + 3].split('=')[1].split('\n')[0])],
                                'line_machine' : [ file.split('/')[-3]],
                                'program_name' : [os.path.basename(file)]
                            })
                            self.save_position_sp(position_sp)

                            logger.success(
                                f"Convert file to database table smt_print_program_log_printcondition(121), smt_print_program_log_cleaningcondition(121), smt_print_program_log_printposition(121)",
                                input_process=f"file {file}",
                                output_process=f"database table smt_print_program_log_printcondition(121), smt_print_program_log_cleaningcondition(121), smt_print_program_log_printposition(121)"
                            )
                            self.move_to_backup(file)
                            
                    except Exception as ex:
                        logger.exception(ex)
                        self.move_to_error(file)
                        
        logger.log("STOP", None)

    def handle(self, *args, **options):   
        self.run()      
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)