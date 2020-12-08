# Example program to save several sensor data including bounding box
# Sensors: RGB Camera (+BoundingBox), De[th Camera, Segmentation Camera, Lidar Camera
# By Mukhlas Adib
# 2020
# Last tested on CARLA 0.9.10.1

# CARLA Simulator is licensed under the terms of the MIT license
# For a copy, see <https://opensource.org/licenses/MIT>
# For more information about CARLA Simulator, visit https://carla.org/

import sys
import time
import argparse
import logging
import random
import queue
import math
import psutil

from queue import Queue
from datetime import datetime
from subprocess import Popen
from typing import List
# pylint: disable= no-name-in-module
from win32process import DETACHED_PROCESS

try:
    # sys.path.append(glob.glob('../carla/dist/carla-*%d.%d-%s.egg' % (
    #     sys.version_info.major,
    #     sys.version_info.minor,
    #     'win-amd64' if os.name == 'nt' else 'linux-x86_64'))[0])
    sys.path.append("C:/Studium/DLVR/CARLA/PythonAPI/carla/dist/carla-0.9.10-py3.7-win-amd64.egg")
except IndexError:
    print('carla not found')
# pylint: disable= import-error, wrong-import-position
# noinspection PyUnresolvedReferences
import carla
import carla_vehicle_annotator as cva


def retrieve_data(sensor_queue, frame, timeout=5):
    while True:
        try:
            data = sensor_queue.get(True, timeout)
        except queue.Empty:
            return None
        if data.frame == frame:
            return data


SAVE_RGB = True
SAVE_DEPTH = False
SAVE_SEGM = False
SAVE_LIDAR = False
TICK_SENSOR = 1

all_id = []
walkers_list = []


def main():
    argparser = argparse.ArgumentParser(
        description=__doc__)
    argparser.add_argument(
        '--host',
        metavar='H',
        default='127.0.0.1',
        help='IP of the host server (default: 127.0.0.1)')
    argparser.add_argument(
        '-p', '--port',
        metavar='P',
        default=2000,
        type=int,
        help='TCP port to listen to (default: 2000)')
    argparser.add_argument(
        '-n', '--number-of-vehicles',
        metavar='N',
        default=50,
        type=int,
        help='number of vehicles (default: 50)')
    argparser.add_argument(
        '-tm_p', '--tm_port',
        metavar='P',
        default=8000,
        type=int,
        help='port to communicate with TM (default: 8000)')
    argparser.add_argument(
        '-ec', '--start_carla',
        default=False,
        type=bool,
        help='Start and end CARLA automatically')
    argparser.add_argument(
        '-mi', '--max_images',
        default=2_000,
        type=int,
        help='Number of images captured before the script ends')
    argparser.add_argument(
        '-mt', '--max_time',
        default=30,
        type=int,
        help='Minutes before the script ends')

    args = argparser.parse_args()

    if args.start_carla:
        _ = Popen([r'C:\Studium\DLVR\\CARLA\\CarlaUE4.exe'], creationflags=DETACHED_PROCESS).pid
        time.sleep(5)

    vehicles_list = []
    nonvehicles_list = []
    client = carla.Client(args.host, args.port)
    client.set_timeout(10.0)

    start_time = datetime.now()

    try:

        # region setup
        traffic_manager = client.get_trafficmanager(args.tm_port)
        traffic_manager.set_global_distance_to_leading_vehicle(2.0)
        world = client.get_world()

        print('\nRUNNING in synchronous mode\n')
        settings = world.get_settings()
        traffic_manager.set_synchronous_mode(True)
        if not settings.synchronous_mode:
            synchronous_master = True
            settings.synchronous_mode = True
            settings.fixed_delta_seconds = 0.05
            world.apply_settings(settings)
        else:
            synchronous_master = False

        blueprints: carla.BlueprintLibrary = world.get_blueprint_library().filter('vehicle.*')

        spawn_points: List[carla.Transform] = world.get_map().get_spawn_points()
        number_of_spawn_points = len(spawn_points)

        if args.number_of_vehicles < number_of_spawn_points:
            random.shuffle(spawn_points)
        elif args.number_of_vehicles > number_of_spawn_points:
            msg = 'Requested %d vehicles, but could only find %d spawn points'
            logging.warning(msg, args.number_of_vehicles, number_of_spawn_points)
            args.number_of_vehicles = number_of_spawn_points

        SpawnActor = carla.command.SpawnActor
        SetAutopilot = carla.command.SetAutopilot
        FutureActor = carla.command.FutureActor

        images_path, labels_path = cva.setup_data_directory()
        # endregion

        # region Spawn ego vehicle and sensors

        q_list: List[Queue] = []
        idx: int = 0

        tick_queue: Queue = Queue()
        world.on_tick(tick_queue.put)
        q_list.append(tick_queue)
        tick_idx: int = idx
        idx = idx + 1

        # Spawn ego vehicle
        ego_bp = random.choice(blueprints)
        ego_transform = random.choice(spawn_points)
        ego_vehicle = world.spawn_actor(ego_bp, ego_transform)
        vehicles_list.append(ego_vehicle)
        ego_vehicle.set_autopilot(True)
        print('Ego-vehicle ready')

        # Spawn RGB camera
        cam_transform = carla.Transform(carla.Location(x=1.5, z=2.4))
        cam_bp = world.get_blueprint_library().find('sensor.camera.rgb')
        cam_bp.set_attribute('sensor_tick', str(TICK_SENSOR))
        cam = world.spawn_actor(cam_bp, cam_transform, attach_to=ego_vehicle)
        nonvehicles_list.append(cam)
        cam_queue = queue.Queue()
        cam.listen(cam_queue.put)
        q_list.append(cam_queue)
        cam_idx = idx
        idx = idx + 1
        print('RGB camera ready')

        # Spawn depth camera
        depth_bp = world.get_blueprint_library().find('sensor.camera.depth')
        depth_bp.set_attribute('sensor_tick', str(TICK_SENSOR))
        depth = world.spawn_actor(depth_bp, cam_transform, attach_to=ego_vehicle)
        cc_depth_log = carla.ColorConverter.LogarithmicDepth
        nonvehicles_list.append(depth)
        depth_queue = queue.Queue()
        depth.listen(depth_queue.put)
        q_list.append(depth_queue)
        depth_idx = idx
        idx = idx + 1
        print('Depth camera ready')

        # Spawn segmentation camera
        if SAVE_SEGM:
            segm_bp = world.get_blueprint_library().find('sensor.camera.semantic_segmentation')
            segm_bp.set_attribute('sensor_tick', str(TICK_SENSOR))
            segm_transform = carla.Transform(carla.Location(x=1.5, z=2.4))
            segm = world.spawn_actor(segm_bp, segm_transform, attach_to=ego_vehicle)
            cc_segm = carla.ColorConverter.CityScapesPalette
            nonvehicles_list.append(segm)
            segm_queue = queue.Queue()
            segm.listen(segm_queue.put)
            q_list.append(segm_queue)
            segm_idx = idx
            idx = idx + 1
            print('Segmentation camera ready')

        # Spawn LIDAR sensor
        if SAVE_LIDAR:
            lidar_bp = world.get_blueprint_library().find('sensor.lidar.ray_cast')
            lidar_bp.set_attribute('sensor_tick', str(TICK_SENSOR))
            lidar_bp.set_attribute('channels', '64')
            lidar_bp.set_attribute('points_per_second', '1120000')
            lidar_bp.set_attribute('upper_fov', '30')
            lidar_bp.set_attribute('range', '100')
            lidar_bp.set_attribute('rotation_frequency', '20')
            lidar_transform = carla.Transform(carla.Location(x=0, z=4.0))
            lidar = world.spawn_actor(lidar_bp, lidar_transform, attach_to=ego_vehicle)
            nonvehicles_list.append(lidar)
            lidar_queue = queue.Queue()
            lidar.listen(lidar_queue.put)
            q_list.append(lidar_queue)
            lidar_idx = idx
            idx = idx + 1
            print('LIDAR ready')

        # endregion

        # region Spawn vehicles
        batch = []
        number_of_bikes = number_of_cars = number_of_motorbikes = int(math.ceil(args.number_of_vehicles * 0.25))
        number_of_trucks = args.number_of_vehicles - number_of_bikes - number_of_cars - number_of_motorbikes
        truck_blueprints = [blueprints.find("vehicle.tesla.cybertruck"), blueprints.find("vehicle.carlamotors.carlacola")]
        car_blueprints_ids = [
            "vehicle.citroen.c3",
            "vehicle.chevrolet.impala",
            "vehicle.audi.a2",
            "vehicle.nissan.micra",
            "vehicle.audi.tt",
            "vehicle.bmw.grandtourer",
            "vehicle.bmw.isetta",
            "vehicle.dodge_charger.police",
            "vehicle.jeep.wrangler_rubicon",
            "vehicle.mercedes-benz.coupe",
            "vehicle.mini.cooperst",
            "vehicle.nissan.patrol",
            "vehicle.seat.leon",
            "vehicle.toyota.prius",
            "vehicle.tesla.model3",
            "vehicle.audi.etron",
            "vehicle.volkswagen.t2",
            "vehicle.lincoln.mkz2017",
            "vehicle.mustang.mustang"
        ]
        car_blueprints = [blueprints.find(i) for i in car_blueprints_ids]
        bike_blueprints_ids = [
            "vehicle.bh.crossbike",
            "vehicle.gazelle.omafiets",
            "vehicle.diamondback.century"
        ]
        bike_blueprints = [blueprints.find(i) for i in bike_blueprints_ids]
        motorbike_blueprints_ids = [
            "vehicle.harley-davidson.low_rider",
            "vehicle.yamaha.yzf",
            "vehicle.kawasaki.ninja"
        ]
        motorbike_blueprints = [blueprints.find(i) for i in motorbike_blueprints_ids]

        i = 0
        for n in range(number_of_trucks):
            blueprint = random.choice(truck_blueprints)
            if blueprint.has_attribute('color'):
                color = random.choice(blueprint.get_attribute('color').recommended_values)
                blueprint.set_attribute('color', color)
            if blueprint.has_attribute('driver_id'):
                driver_id = random.choice(blueprint.get_attribute('driver_id').recommended_values)
                blueprint.set_attribute('driver_id', driver_id)
            blueprint.set_attribute('role_name', 'autopilot')
            batch.append(SpawnActor(blueprint, spawn_points[i]).then(SetAutopilot(FutureActor, True)))
            i += 1

        for n in range(number_of_cars):
            blueprint = random.choice(car_blueprints)
            if blueprint.has_attribute('color'):
                color = random.choice(blueprint.get_attribute('color').recommended_values)
                blueprint.set_attribute('color', color)
            if blueprint.has_attribute('driver_id'):
                driver_id = random.choice(blueprint.get_attribute('driver_id').recommended_values)
                blueprint.set_attribute('driver_id', driver_id)
            blueprint.set_attribute('role_name', 'autopilot')
            batch.append(SpawnActor(blueprint, spawn_points[i]).then(SetAutopilot(FutureActor, True)))
            i += 1

        for n in range(number_of_motorbikes):
            blueprint = random.choice(motorbike_blueprints)
            if blueprint.has_attribute('color'):
                color = random.choice(blueprint.get_attribute('color').recommended_values)
                blueprint.set_attribute('color', color)
            if blueprint.has_attribute('driver_id'):
                driver_id = random.choice(blueprint.get_attribute('driver_id').recommended_values)
                blueprint.set_attribute('driver_id', driver_id)
            blueprint.set_attribute('role_name', 'autopilot')
            batch.append(SpawnActor(blueprint, spawn_points[i]).then(SetAutopilot(FutureActor, True)))
            i += 1

        for n in range(number_of_bikes):
            blueprint = random.choice(bike_blueprints)
            if blueprint.has_attribute('color'):
                color = random.choice(blueprint.get_attribute('color').recommended_values)
                blueprint.set_attribute('color', color)
            if blueprint.has_attribute('driver_id'):
                driver_id = random.choice(blueprint.get_attribute('driver_id').recommended_values)
                blueprint.set_attribute('driver_id', driver_id)
            blueprint.set_attribute('role_name', 'autopilot')
            batch.append(SpawnActor(blueprint, spawn_points[i]).then(SetAutopilot(FutureActor, True)))
            i += 1

        for response in client.apply_batch_sync(batch, synchronous_master):
            if response.error:
                logging.error(response.error)
            else:
                vehicles_list.append(response.actor_id)

        print('Created %d npc vehicles \n' % len(vehicles_list))
        # endregion

        # region Spawn Walkers

        # some settings
        percentagePedestriansRunning = 0.0  # how many pedestrians will run
        percentagePedestriansCrossing = 0.0  # how many pedestrians will walk through the road
        blueprintsWalkers = world.get_blueprint_library().filter("walker.pedestrian.*")
        # 1. take all the random locations to spawn
        spawn_points = []
        for i in range(20):
            spawn_point = carla.Transform()
            loc = world.get_random_location_from_navigation()
            if (loc != None):
                spawn_point.location = loc
                spawn_points.append(spawn_point)
        # 2. we spawn the walker object
        batch = []
        walker_speed = []
        for spawn_point in spawn_points:
            walker_bp = random.choice(blueprintsWalkers)
            # set as not invincible
            if walker_bp.has_attribute('is_invincible'):
                walker_bp.set_attribute('is_invincible', 'false')
            # set the max speed
            if walker_bp.has_attribute('speed'):
                if (random.random() > percentagePedestriansRunning):
                    # walking
                    walker_speed.append(walker_bp.get_attribute('speed').recommended_values[1])
                else:
                    # running
                    walker_speed.append(walker_bp.get_attribute('speed').recommended_values[2])
            else:
                print("Walker has no speed")
                walker_speed.append(0.0)
            batch.append(SpawnActor(walker_bp, spawn_point))
        results = client.apply_batch_sync(batch, True)
        walker_speed2 = []
        for i in range(len(results)):
            if results[i].error:
                logging.error(results[i].error)
            else:
                walkers_list.append({"id": results[i].actor_id})
                walker_speed2.append(walker_speed[i])
        walker_speed = walker_speed2
        # 3. we spawn the walker controller
        batch = []
        walker_controller_bp = world.get_blueprint_library().find('controller.ai.walker')
        for i in range(len(walkers_list)):
            batch.append(SpawnActor(walker_controller_bp, carla.Transform(), walkers_list[i]["id"]))
        results = client.apply_batch_sync(batch, True)
        for i in range(len(results)):
            if results[i].error:
                logging.error(results[i].error)
            else:
                walkers_list[i]["con"] = results[i].actor_id
        # 4. we put altogether the walkers and controllers id to get the objects from their id
        for i in range(len(walkers_list)):
            all_id.append(walkers_list[i]["con"])
            all_id.append(walkers_list[i]["id"])
        all_actors = world.get_actors(all_id)

        # wait for a tick to ensure client receives the last transform of the walkers we have just created
        # if not args.sync or not synchronous_master:
        #     world.wait_for_tick()
        # else:
        #     world.tick()

        # 5. initialize each controller and set target to walk to (list is [controler, actor, controller, actor ...])
        # set how many pedestrians can cross the road
        world.set_pedestrians_cross_factor(percentagePedestriansCrossing)
        for i in range(0, len(all_id), 2):
            # start walker
            all_actors[i].start()
            # set walk to random point
            all_actors[i].go_to_location(world.get_random_location_from_navigation())
            # max speed
            all_actors[i].set_max_speed(float(walker_speed[int(i / 2)]))
        # endregion

        # Begin the loop
        time_sim = 0
        images_captured: int = 0
        time_limit_reached: bool = False
        while images_captured < args.max_images and not time_limit_reached:
            # Extract the available data
            now_frame = world.tick()

            # Check whether it's time to capture data
            if time_sim >= TICK_SENSOR:

                
                minutes_passed = (datetime.now() - start_time).total_seconds() / 60
                if minutes_passed >= args.max_time:
                    time_limit_reached = True

                data = [retrieve_data(q, now_frame) for q in q_list]
                assert all(x.frame == now_frame for x in data if x is not None)

                # Skip if any sensor data is not available
                if None in data:
                    continue

                # vehicles_raw = world.get_actors().filter('vehicle.*')
                all_actors = world.get_actors()
                vehicles_raw = []
                for actor in all_actors:
                    if actor.type_id.startswith('walker.pedestrian') or actor.type_id.startswith('vehicle'):
                        vehicles_raw.append(actor)

                # vehicles_raw = world.get_actors().filter('walker.pedestrian.*')
                snap = data[tick_idx]
                rgb_img = data[cam_idx]
                depth_img = data[depth_idx]

                # Attach additional information to the snapshot
                vehicles = cva.snap_processing(vehicles_raw, snap)

                # Save depth image, RGB image, and Bounding Boxes data
                if SAVE_DEPTH:
                    depth_img.save_to_disk('out_depth/%06d.png' % depth_img.frame, cc_depth_log)
                depth_meter = cva.extract_depth(depth_img)
                filtered, removed = cva.auto_annotate(vehicles, cam, depth_meter, max_dist=50,
                                                      json_path='vehicle_class_carla.json')
                bboxes = filtered['bbox']
                classes = filtered['class']
                big_enough_bboxes = []
                filtered_classes = []
                for b, c in zip(bboxes, classes):
                    # Dict with:
                    # bbox: [[min_x, min_y], [max_x, max_y]]
                    # vehicles
                    x1 = b[0][0]
                    x2 = b[1][0]
                    y1 = b[0][1]
                    y2 = b[1][1]
                    delta_x = x1 - x2 if x1 > x2 else x2 - x1
                    delta_y = y1 - y2 if y1 > y2 else y2 - y1
                    area: float = delta_x * delta_y

                    if area > 350:
                        big_enough_bboxes.append(b)
                        filtered_classes.append(c)

                if len(big_enough_bboxes) > 0:
                    images_captured += 1
                    if images_captured % 10 == 0:
                        print(f"Captured {images_captured}/{args.max_images} in {math.ceil(minutes_passed)} minutes")

                    cva.save_output(rgb_img, big_enough_bboxes, filtered_classes, removed['bbox'], removed['class'], save_patched=True, out_format='json')
                    cva.save2darknet(bboxes=big_enough_bboxes, vehicle_class=filtered_classes, carla_img=rgb_img, save_train=True, images_path=images_path, labels_path=labels_path)

                    # Save segmentation image
                    if SAVE_SEGM:
                        segm_img = data[segm_idx]
                        segm_img.save_to_disk('out_segm/%06d.png' % segm_img.frame, cc_segm)

                    # Save LIDAR data
                    if SAVE_LIDAR:
                        lidar_data = data[lidar_idx]
                        lidar_data.save_to_disk('out_lidar/%06d.ply' % segm_img.frame)
                

                time_sim = 0
            time_sim = time_sim + settings.fixed_delta_seconds

    finally:
        # cva.save2darknet(None, None, None, save_train=True)
        try:
            cam.stop()
            depth.stop()
            if SAVE_SEGM:
                segm.stop()
            if SAVE_LIDAR:
                lidar.stop()
        except:
            print("Simulation ended before sensors have been created")

        settings = world.get_settings()
        settings.synchronous_mode = False
        settings.fixed_delta_seconds = None
        world.apply_settings(settings)

        print('\ndestroying %d vehicles' % len(vehicles_list))
        client.apply_batch([carla.command.DestroyActor(x) for x in vehicles_list])

        print('destroying %d nonvehicles' % len(nonvehicles_list))
        client.apply_batch([carla.command.DestroyActor(x) for x in nonvehicles_list])

        # stop walker controllers (list is [controller, actor, controller, actor ...])
        # for i in range(0, len(all_id), 2):
        #     all_actors[i].stop()

        print('\ndestroying %d walkers' % len(walkers_list))
        client.apply_batch([carla.command.DestroyActor(x) for x in all_id])

        time.sleep(0.5)


if __name__ == '__main__':

    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        print('\ndone.')
