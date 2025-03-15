"""
空间索引实现，用于高效的3D空间查询和碰撞检测。
基于自适应多分辨率网格设计。
"""
import time
import uuid
import math
import numpy as np
from typing import Dict, List, Set, Tuple, Optional, Any
from dataclasses import dataclass, field

from ..common.models import Position, Vector, Vehicle
from ..common.utils import get_logger, Timer

logger = get_logger(__name__)

@dataclass
class GridCell:
    """网格单元"""
    level: int  # 网格级别，0为最粗粒度
    grid_id: Tuple[int, int, int]  # 网格坐标 (x, y, z)
    vehicles: Set[str] = field(default_factory=set)  # 该网格中的车辆ID集合
    last_update: float = field(default_factory=time.time)  # 最后更新时间
    
    @property
    def vehicle_count(self) -> int:
        """获取车辆数量"""
        return len(self.vehicles)


class SpatialIndex:
    """空间索引实现，基于自适应多分辨率网格"""
    
    def __init__(self, 
                 base_size: Tuple[float, float, float] = (1000.0, 1000.0, 100.0),
                 min_size: Tuple[float, float, float] = (10.0, 10.0, 5.0),
                 max_level: int = 3,
                 density_threshold_split: int = 50,
                 density_threshold_merge: int = 10,
                 adjustment_interval: float = 10.0):
        """
        初始化空间索引
        
        Args:
            base_size: 基础网格大小 (x, y, z)，单位为米
            min_size: 最小网格大小 (x, y, z)，单位为米
            max_level: 最大网格级别
            density_threshold_split: 分裂阈值（车辆数量）
            density_threshold_merge: 合并阈值（车辆数量）
            adjustment_interval: 网格调整间隔（秒）
        """
        self.base_size = base_size
        self.min_size = min_size
        self.max_level = max_level
        self.density_threshold_split = density_threshold_split
        self.density_threshold_merge = density_threshold_merge
        self.adjustment_interval = adjustment_interval
        
        # 网格数据结构
        self.grids: Dict[int, Dict[Tuple[int, int, int], GridCell]] = {}  # level -> {grid_id -> GridCell}
        self.vehicle_to_grid: Dict[str, Tuple[int, Tuple[int, int, int]]] = {}  # vehicle_id -> (level, grid_id)
        
        # 车辆位置缓存
        self.vehicle_positions: Dict[str, Position] = {}  # vehicle_id -> Position
        
        # 统计信息
        self.stats = {
            "total_vehicles": 0,
            "grid_cells": 0,
            "max_vehicles_per_cell": 0,
            "last_adjustment": time.time()
        }
        
        # 初始化网格
        for level in range(max_level + 1):
            self.grids[level] = {}
        
        logger.info(f"Spatial index initialized with base size {base_size}, max level {max_level}")
    
    def get_cell_size(self, level: int) -> Tuple[float, float, float]:
        """
        获取指定级别的网格单元大小
        
        Args:
            level: 网格级别
            
        Returns:
            网格单元大小 (x, y, z)
        """
        factor = 2 ** level
        return (
            self.base_size[0] / factor,
            self.base_size[1] / factor,
            self.base_size[2] / factor
        )
    
    def get_grid_id(self, position: Position, level: int) -> Tuple[int, int, int]:
        """
        计算给定位置和级别的网格ID
        
        Args:
            position: 位置
            level: 网格级别
            
        Returns:
            网格ID (x, y, z)
        """
        cell_size = self.get_cell_size(level)
        x = int(position.x / cell_size[0])
        y = int(position.y / cell_size[1])
        z = int(position.z / cell_size[2])
        return (x, y, z)
    
    def get_grid_level(self, position: Position) -> int:
        """
        确定给定位置应该使用的网格级别
        
        Args:
            position: 位置
            
        Returns:
            网格级别
        """
        # 从最细粒度开始检查
        for level in range(self.max_level, -1, -1):
            grid_id = self.get_grid_id(position, level)
            
            # 如果该级别的网格存在
            if grid_id in self.grids.get(level, {}):
                cell = self.grids[level][grid_id]
                
                # 检查是否应该使用该级别
                if self._should_use_level(cell.vehicle_count, level):
                    return level
        
        # 默认使用最粗粒度网格
        return 0
    
    def _should_use_level(self, vehicle_count: int, level: int) -> bool:
        """
        根据车辆数量决定是否应该使用该级别
        
        Args:
            vehicle_count: 车辆数量
            level: 网格级别
            
        Returns:
            是否应该使用该级别
        """
        if level == 0:
            # 最粗粒度级别，如果车辆数量小于分裂阈值，则使用
            return vehicle_count < self.density_threshold_split
        
        if level == self.max_level:
            # 最细粒度级别，如果车辆数量大于合并阈值，则使用
            return vehicle_count > self.density_threshold_merge
        
        # 中间级别，如果车辆数量在合并阈值和分裂阈值之间，则使用
        return (vehicle_count > self.density_threshold_merge and 
                vehicle_count < self.density_threshold_split)
    
    def insert_vehicle(self, vehicle_id: str, position: Position) -> None:
        """
        插入车辆到适当的网格
        
        Args:
            vehicle_id: 车辆ID
            position: 车辆位置
        """
        # 更新车辆位置缓存
        self.vehicle_positions[vehicle_id] = position
        
        # 确定应该使用的网格级别
        level = self.get_grid_level(position)
        grid_id = self.get_grid_id(position, level)
        
        # 从旧网格中移除
        self.remove_vehicle(vehicle_id)
        
        # 添加到新网格
        if grid_id not in self.grids[level]:
            self.grids[level][grid_id] = GridCell(level=level, grid_id=grid_id)
            self.stats["grid_cells"] += 1
        
        cell = self.grids[level][grid_id]
        cell.vehicles.add(vehicle_id)
        cell.last_update = time.time()
        
        # 更新映射
        self.vehicle_to_grid[vehicle_id] = (level, grid_id)
        
        # 更新统计信息
        self.stats["total_vehicles"] = len(self.vehicle_positions)
        self.stats["max_vehicles_per_cell"] = max(
            self.stats["max_vehicles_per_cell"], 
            cell.vehicle_count
        )
        
        # 检查是否需要调整网格
        self._check_adjustment()
    
    def remove_vehicle(self, vehicle_id: str) -> None:
        """
        从网格中移除车辆
        
        Args:
            vehicle_id: 车辆ID
        """
        if vehicle_id in self.vehicle_to_grid:
            level, grid_id = self.vehicle_to_grid[vehicle_id]
            
            if level in self.grids and grid_id in self.grids[level]:
                cell = self.grids[level][grid_id]
                cell.vehicles.discard(vehicle_id)
                cell.last_update = time.time()
                
                # 如果网格为空，移除它
                if not cell.vehicles:
                    del self.grids[level][grid_id]
                    self.stats["grid_cells"] -= 1
            
            del self.vehicle_to_grid[vehicle_id]
        
        # 从位置缓存中移除
        if vehicle_id in self.vehicle_positions:
            del self.vehicle_positions[vehicle_id]
            self.stats["total_vehicles"] = len(self.vehicle_positions)
    
    def get_nearby_vehicles(self, position: Position, radius: float) -> Set[str]:
        """
        获取给定位置附近的车辆
        
        Args:
            position: 位置
            radius: 搜索半径（米）
            
        Returns:
            附近车辆ID集合
        """
        # 确定应该使用的网格级别
        level = self.get_grid_level(position)
        grid_id = self.get_grid_id(position, level)
        cell_size = self.get_cell_size(level)
        
        # 计算需要检查的网格范围
        grid_radius_x = int(radius / cell_size[0]) + 1
        grid_radius_y = int(radius / cell_size[1]) + 1
        grid_radius_z = int(radius / cell_size[2]) + 1
        
        nearby_vehicles = set()
        
        # 检查相邻网格
        for dx in range(-grid_radius_x, grid_radius_x + 1):
            for dy in range(-grid_radius_y, grid_radius_y + 1):
                for dz in range(-grid_radius_z, grid_radius_z + 1):
                    neighbor_id = (grid_id[0] + dx, grid_id[1] + dy, grid_id[2] + dz)
                    
                    if neighbor_id in self.grids[level]:
                        nearby_vehicles.update(self.grids[level][neighbor_id].vehicles)
        
        # 过滤掉超出半径的车辆
        result = set()
        for vehicle_id in nearby_vehicles:
            if vehicle_id in self.vehicle_positions:
                vehicle_pos = self.vehicle_positions[vehicle_id]
                distance = self._calculate_distance(position, vehicle_pos)
                
                if distance <= radius:
                    result.add(vehicle_id)
        
        return result
    
    def get_vehicle_position(self, vehicle_id: str) -> Optional[Position]:
        """
        获取车辆位置
        
        Args:
            vehicle_id: 车辆ID
            
        Returns:
            车辆位置，如果不存在则返回None
        """
        return self.vehicle_positions.get(vehicle_id)
    
    def _calculate_distance(self, pos1: Position, pos2: Position) -> float:
        """
        计算两点之间的距离
        
        Args:
            pos1: 位置1
            pos2: 位置2
            
        Returns:
            距离（米）
        """
        return math.sqrt(
            (pos1.x - pos2.x) ** 2 +
            (pos1.y - pos2.y) ** 2 +
            (pos1.z - pos2.z) ** 2
        )
    
    def _check_adjustment(self) -> None:
        """检查是否需要调整网格"""
        now = time.time()
        if now - self.stats["last_adjustment"] >= self.adjustment_interval:
            self.adjust_grid_resolution()
            self.stats["last_adjustment"] = now
    
    def adjust_grid_resolution(self) -> None:
        """根据车辆密度调整网格分辨率"""
        with Timer() as timer:
            # 收集需要分裂和合并的网格
            to_split = []
            to_merge = []
            
            # 检查每个级别的网格
            for level in range(self.max_level + 1):
                for grid_id, cell in list(self.grids.get(level, {}).items()):
                    # 检查是否需要分裂
                    if level < self.max_level and cell.vehicle_count > self.density_threshold_split:
                        to_split.append((level, grid_id))
                    
                    # 检查是否需要合并
                    elif level > 0 and cell.vehicle_count < self.density_threshold_merge:
                        to_merge.append((level, grid_id))
            
            # 执行分裂
            for level, grid_id in to_split:
                self._split_grid(level, grid_id)
            
            # 执行合并
            for level, grid_id in to_merge:
                self._merge_grid(level, grid_id)
        
        logger.debug(f"Grid adjustment completed in {timer.elapsed_ms:.2f}ms, "
                    f"split {len(to_split)}, merge {len(to_merge)}")
    
    def _split_grid(self, level: int, grid_id: Tuple[int, int, int]) -> None:
        """
        将网格细分为更高分辨率
        
        Args:
            level: 网格级别
            grid_id: 网格ID
        """
        if level not in self.grids or grid_id not in self.grids[level]:
            return
        
        cell = self.grids[level][grid_id]
        vehicles = list(cell.vehicles)
        
        # 移除原网格
        del self.grids[level][grid_id]
        self.stats["grid_cells"] -= 1
        
        # 重新插入车辆，让它们进入更细粒度的网格
        for vehicle_id in vehicles:
            if vehicle_id in self.vehicle_positions:
                position = self.vehicle_positions[vehicle_id]
                
                # 强制使用更高级别
                new_level = level + 1
                new_grid_id = self.get_grid_id(position, new_level)
                
                # 添加到新网格
                if new_grid_id not in self.grids[new_level]:
                    self.grids[new_level][new_grid_id] = GridCell(level=new_level, grid_id=new_grid_id)
                    self.stats["grid_cells"] += 1
                
                self.grids[new_level][new_grid_id].vehicles.add(vehicle_id)
                self.grids[new_level][new_grid_id].last_update = time.time()
                
                # 更新映射
                self.vehicle_to_grid[vehicle_id] = (new_level, new_grid_id)
    
    def _merge_grid(self, level: int, grid_id: Tuple[int, int, int]) -> None:
        """
        将网格合并为更低分辨率
        
        Args:
            level: 网格级别
            grid_id: 网格ID
        """
        if level not in self.grids or grid_id not in self.grids[level]:
            return
        
        cell = self.grids[level][grid_id]
        vehicles = list(cell.vehicles)
        
        # 移除原网格
        del self.grids[level][grid_id]
        self.stats["grid_cells"] -= 1
        
        # 重新插入车辆，让它们进入更粗粒度的网格
        for vehicle_id in vehicles:
            if vehicle_id in self.vehicle_positions:
                position = self.vehicle_positions[vehicle_id]
                
                # 强制使用更低级别
                new_level = level - 1
                new_grid_id = self.get_grid_id(position, new_level)
                
                # 添加到新网格
                if new_grid_id not in self.grids[new_level]:
                    self.grids[new_level][new_grid_id] = GridCell(level=new_level, grid_id=new_grid_id)
                    self.stats["grid_cells"] += 1
                
                self.grids[new_level][new_grid_id].vehicles.add(vehicle_id)
                self.grids[new_level][new_grid_id].last_update = time.time()
                
                # 更新映射
                self.vehicle_to_grid[vehicle_id] = (new_level, new_grid_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取空间索引统计信息
        
        Returns:
            统计信息字典
        """
        # 更新统计信息
        level_stats = {}
        for level in range(self.max_level + 1):
            level_stats[level] = {
                "cells": len(self.grids.get(level, {})),
                "vehicles": sum(cell.vehicle_count for cell in self.grids.get(level, {}).values())
            }
        
        return {
            **self.stats,
            "levels": level_stats
        }


class SpatialPartitioner:
    """空间分区器，用于数据分片"""
    
    def __init__(self, 
                 spatial_index: SpatialIndex,
                 num_shards: int = 10,
                 min_load: float = 0.3,
                 max_load: float = 0.7,
                 rebalance_interval: float = 60.0):
        """
        初始化空间分区器
        
        Args:
            spatial_index: 空间索引
            num_shards: 初始分片数量
            min_load: 最小负载阈值
            max_load: 最大负载阈值
            rebalance_interval: 重新平衡间隔（秒）
        """
        self.spatial_index = spatial_index
        self.num_shards = num_shards
        self.min_load = min_load
        self.max_load = max_load
        self.rebalance_interval = rebalance_interval
        
        # 分片数据结构
        self.regions: Dict[str, List[Tuple[int, Tuple[int, int, int]]]] = {}  # region_id -> [(level, grid_id), ...]
        self.region_to_shard: Dict[str, str] = {}  # region_id -> shard_id
        self.shard_loads: Dict[str, float] = {}  # shard_id -> load
        
        # 初始化分片
        self._initialize_shards()
        
        # 统计信息
        self.stats = {
            "total_shards": num_shards,
            "total_regions": 0,
            "last_rebalance": time.time()
        }
        
        logger.info(f"Spatial partitioner initialized with {num_shards} shards")
    
    def _initialize_shards(self) -> None:
        """初始化分片"""
        # 创建初始分片
        for i in range(self.num_shards):
            shard_id = f"shard-{i}"
            self.shard_loads[shard_id] = 0.0
        
        # 创建初始区域（每个顶层网格一个区域）
        for grid_id in self.spatial_index.grids.get(0, {}):
            region_id = f"region-{uuid.uuid4()}"
            self.regions[region_id] = [(0, grid_id)]
            
            # 分配到分片
            shard_id = f"shard-{hash(grid_id) % self.num_shards}"
            self.region_to_shard[region_id] = shard_id
        
        self.stats["total_regions"] = len(self.regions)
    
    def get_shard_for_position(self, position: Position) -> Optional[str]:
        """
        获取给定位置所在的分片
        
        Args:
            position: 位置
            
        Returns:
            分片ID，如果不存在则返回None
        """
        # 找到位置所在的网格
        level = self.spatial_index.get_grid_level(position)
        grid_id = self.spatial_index.get_grid_id(position, level)
        
        # 找到包含该网格的区域
        region_id = self._find_region_for_grid(level, grid_id)
        
        # 如果找不到区域，尝试使用父网格
        if not region_id and level > 0:
            for parent_level in range(level - 1, -1, -1):
                parent_grid_id = self._get_parent_grid_id(grid_id, level, parent_level)
                region_id = self._find_region_for_grid(parent_level, parent_grid_id)
                if region_id:
                    break
        
        # 返回负责该区域的分片
        return self.region_to_shard.get(region_id)
    
    def _find_region_for_grid(self, level: int, grid_id: Tuple[int, int, int]) -> Optional[str]:
        """
        找到包含给定网格的区域
        
        Args:
            level: 网格级别
            grid_id: 网格ID
            
        Returns:
            区域ID，如果不存在则返回None
        """
        for region_id, grids in self.regions.items():
            if (level, grid_id) in grids:
                return region_id
        return None
    
    def _get_parent_grid_id(self, grid_id: Tuple[int, int, int], 
                           current_level: int, parent_level: int) -> Tuple[int, int, int]:
        """
        获取父网格ID
        
        Args:
            grid_id: 当前网格ID
            current_level: 当前网格级别
            parent_level: 父网格级别
            
        Returns:
            父网格ID
        """
        level_diff = current_level - parent_level
        factor = 2 ** level_diff
        return (
            grid_id[0] // factor,
            grid_id[1] // factor,
            grid_id[2] // factor
        )
    
    def update_load(self, shard_id: str, load: float) -> None:
        """
        更新分片负载
        
        Args:
            shard_id: 分片ID
            load: 负载值（0.0-1.0）
        """
        if shard_id in self.shard_loads:
            self.shard_loads[shard_id] = load
    
    def check_rebalance(self) -> bool:
        """
        检查是否需要重新平衡分片
        
        Returns:
            是否执行了重新平衡
        """
        now = time.time()
        if now - self.stats["last_rebalance"] >= self.rebalance_interval:
            self.rebalance_shards()
            self.stats["last_rebalance"] = now
            return True
        return False
    
    def rebalance_shards(self) -> Dict[str, Any]:
        """
        重新平衡分片
        
        Returns:
            重新平衡结果
        """
        with Timer() as timer:
            # 找出负载过高和过低的分片
            overloaded = [s for s, l in self.shard_loads.items() if l > self.max_load]
            underloaded = [s for s, l in self.shard_loads.items() if l < self.min_load]
            
            # 处理负载过高的分片
            split_regions = []
            for shard_id in overloaded:
                # 找出该分片负责的所有区域
                regions = [r for r, s in self.region_to_shard.items() if s == shard_id]
                
                # 按车辆数量排序
                regions.sort(key=lambda r: self._get_region_vehicle_count(r), reverse=True)
                
                # 分割最大的区域
                if regions:
                    region_id = regions[0]
                    sub_regions = self._split_region(region_id)
                    split_regions.append((region_id, sub_regions))
                    
                    # 为新区域分配分片
                    if sub_regions:
                        # 保留第一个子区域在原分片
                        self.region_to_shard[sub_regions[0]] = shard_id
                        
                        # 为其他子区域分配负载低的分片
                        for i in range(1, len(sub_regions)):
                            if underloaded:
                                under_shard = underloaded.pop(0)
                                self.region_to_shard[sub_regions[i]] = under_shard
                            else:
                                # 如果没有负载低的分片，创建新分片
                                new_shard = self._create_new_shard()
                                self.region_to_shard[sub_regions[i]] = new_shard
            
            # 处理负载过低的分片
            merged_regions = []
            while len(underloaded) >= 2:
                shard1 = underloaded.pop(0)
                shard2 = underloaded.pop(0)
                
                # 找出这两个分片负责的所有区域
                regions1 = [r for r, s in self.region_to_shard.items() if s == shard1]
                regions2 = [r for r, s in self.region_to_shard.items() if s == shard2]
                
                # 尝试合并区域
                merged = False
                for r1 in regions1:
                    for r2 in regions2:
                        if self._can_merge_regions(r1, r2):
                            merged_region = self._merge_regions(r1, r2)
                            merged_regions.append((r1, r2, merged_region))
                            
                            # 更新分片信息
                            self.region_to_shard[merged_region] = shard1
                            
                            # 移除旧区域和分片
                            del self.region_to_shard[r1]
                            del self.region_to_shard[r2]
                            del self.regions[r1]
                            del self.regions[r2]
                            
                            # 标记为已合并
                            merged = True
                            break
                    if merged:
                        break
                
                # 如果没有可合并的区域，放回列表
                if not merged:
                    underloaded.append(shard1)
                    underloaded.append(shard2)
                    break
        
        # 更新统计信息
        self.stats["total_shards"] = len(self.shard_loads)
        self.stats["total_regions"] = len(self.regions)
        
        logger.info(f"Shard rebalance completed in {timer.elapsed_ms:.2f}ms, "
                   f"split {len(split_regions)} regions, merged {len(merged_regions)} regions")
        
        return {
            "overloaded": len(overloaded),
            "underloaded": len(underloaded),
            "split_regions": len(split_regions),
            "merged_regions": len(merged_regions),
            "elapsed_ms": timer.elapsed_ms
        }
    
    def _get_region_vehicle_count(self, region_id: str) -> int:
        """
        获取区域内的车辆数量
        
        Args:
            region_id: 区域ID
            
        Returns:
            车辆数量
        """
        if region_id not in self.regions:
            return 0
        
        count = 0
        for level, grid_id in self.regions[region_id]:
            if level in self.spatial_index.grids and grid_id in self.spatial_index.grids[level]:
                count += self.spatial_index.grids[level][grid_id].vehicle_count
        
        return count
    
    def _split_region(self, region_id: str) -> List[str]:
        """
        分割区域
        
        Args:
            region_id: 区域ID
            
        Returns:
            新区域ID列表
        """
        if region_id not in self.regions:
            return []
        
        grids = self.regions[region_id]
        
        # 如果区域只有一个网格，且级别小于最大级别，则分割该网格
        if len(grids) == 1 and grids[0][0] < self.spatial_index.max_level:
            level, grid_id = grids[0]
            
            # 计算子网格
            child_level = level + 1
            factor = 2
            child_grids = []
            
            for dx in range(factor):
                for dy in range(factor):
                    for dz in range(factor):
                        child_grid_id = (
                            grid_id[0] * factor + dx,
                            grid_id[1] * factor + dy,
                            grid_id[2] * factor + dz
                        )
                        child_grids.append((child_level, child_grid_id))
            
            # 创建新区域
            new_regions = []
            for i, child_grid in enumerate(child_grids):
                new_region_id = f"region-{uuid.uuid4()}"
                self.regions[new_region_id] = [child_grid]
                new_regions.append(new_region_id)
            
            # 移除旧区域
            del self.regions[region_id]
            
            return new_regions
        
        # 如果区域有多个网格，则平均分割
        elif len(grids) > 1:
            # 按车辆数量排序
            sorted_grids = sorted(grids, key=lambda g: 
                                 self.spatial_index.grids.get(g[0], {}).get(g[1], GridCell(g[0], g[1])).vehicle_count,
                                 reverse=True)
            
            # 分成两部分
            mid = len(sorted_grids) // 2
            grids1 = sorted_grids[:mid]
            grids2 = sorted_grids[mid:]
            
            # 创建新区域
            new_region_id1 = f"region-{uuid.uuid4()}"
            new_region_id2 = f"region-{uuid.uuid4()}"
            
            self.regions[new_region_id1] = grids1
            self.regions[new_region_id2] = grids2
            
            # 移除旧区域
            del self.regions[region_id]
            
            return [new_region_id1, new_region_id2]
        
        return []
    
    def _can_merge_regions(self, region_id1: str, region_id2: str) -> bool:
        """
        检查两个区域是否可以合并
        
        Args:
            region_id1: 区域1 ID
            region_id2: 区域2 ID
            
        Returns:
            是否可以合并
        """
        if region_id1 not in self.regions or region_id2 not in self.regions:
            return False
        
        # 检查是否是相邻区域
        grids1 = self.regions[region_id1]
        grids2 = self.regions[region_id2]
        
        # 如果两个区域都只有一个网格，且是同一级别的相邻网格
        if len(grids1) == 1 and len(grids2) == 1:
            level1, grid_id1 = grids1[0]
            level2, grid_id2 = grids2[0]
            
            if level1 == level2:
                # 检查是否相邻
                dx = abs(grid_id1[0] - grid_id2[0])
                dy = abs(grid_id1[1] - grid_id2[1])
                dz = abs(grid_id1[2] - grid_id2[2])
                
                # 如果在一个维度上相邻，其他维度相同
                return (dx + dy + dz == 1)
        
        # 其他情况暂不支持合并
        return False
    
    def _merge_regions(self, region_id1: str, region_id2: str) -> str:
        """
        合并两个区域
        
        Args:
            region_id1: 区域1 ID
            region_id2: 区域2 ID
            
        Returns:
            新区域ID
        """
        if region_id1 not in self.regions or region_id2 not in self.regions:
            return ""
        
        # 创建新区域
        new_region_id = f"region-{uuid.uuid4()}"
        self.regions[new_region_id] = self.regions[region_id1] + self.regions[region_id2]
        
        return new_region_id
    
    def _create_new_shard(self) -> str:
        """
        创建新的分片
        
        Returns:
            新分片ID
        """
        shard_id = f"shard-{uuid.uuid4()}"
        self.shard_loads[shard_id] = 0.0
        self.stats["total_shards"] += 1
        return shard_id
    
    def get_stats(self) -> Dict[str, Any]:
        """
        获取分区器统计信息
        
        Returns:
            统计信息字典
        """
        # 计算每个分片的区域数量和车辆数量
        shard_stats = {}
        for shard_id in self.shard_loads:
            regions = [r for r, s in self.region_to_shard.items() if s == shard_id]
            vehicle_count = sum(self._get_region_vehicle_count(r) for r in regions)
            
            shard_stats[shard_id] = {
                "regions": len(regions),
                "vehicles": vehicle_count,
                "load": self.shard_loads[shard_id]
            }
        
        return {
            **self.stats,
            "shards": shard_stats
        }
